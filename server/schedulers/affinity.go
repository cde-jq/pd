package schedulers

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/unrolled/render"
)

const (
	// LabelName is label scheduler name.
	AffinityName = "affinity-scheduler"
	// LabelType is label scheduler type.
	AffinityType = "affinity"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(AffinityType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*affinitySchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			if len(args) == 0 {
				return errs.ErrSchedulerConfig.FastGenByArgs("affinity args")
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Range = ranges[0]

			return nil
		}
	})

	schedule.RegisterScheduler(AffinityType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &affinitySchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.name = AffinityName
		return newAffinityScheduler(opController, conf, storage), nil
	})
}

type affinitySchedulerConfig struct {
	name  string
	Range core.KeyRange `json:"range"`
}

type affinityScheduler struct {
	*BaseScheduler
	conf    *affinitySchedulerConfig
	storage *core.Storage
}

// LabelScheduler is mainly based on the store's label information for scheduling.
// Now only used for reject leader schedule, that will move the leader out of
// the store with the specific label.
func newAffinityScheduler(opController *schedule.OperatorController, conf *affinitySchedulerConfig, storage *core.Storage) schedule.Scheduler {
	return &affinityScheduler{
		BaseScheduler: NewBaseScheduler(opController),
		conf:          conf,
		storage:       storage,
	}
}

func (s *affinityScheduler) GetName() string {
	return s.conf.name
}

func (s *affinityScheduler) GetType() string {
	return LabelType
}

func (s *affinityScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *affinityScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
	}
	return allowed
}

func targetStore(store_leadernum *map[uint64]uint64) uint64 {
	if len(*store_leadernum) == 0 {
		panic("store_leaders is empty")
	}
	var max uint64 = 0
	var maxstore uint64 = 0
	for storeid, count := range *store_leadernum {
		if count > max {
			max = count
			maxstore = storeid
		} else if count == max && storeid < maxstore {
			maxstore = storeid
		}
	}

	return maxstore
}

func (s *affinityScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	store_leadernum := make(map[uint64]uint64)
	regions := cluster.ScanRegions(s.conf.Range.StartKey, s.conf.Range.EndKey, 0)

	haveLeaderCount := 0
	for _, region := range regions {
		if !isInvolved(region, s.conf.Range.StartKey, s.conf.Range.EndKey) {
			log.Debug(fmt.Sprintf("region %d not involved  start:%s end:%s ,conf start:%s, end:%s",
				region.GetID(), hex.EncodeToString(region.GetStartKey()), hex.EncodeToString(region.GetEndKey()),
				hex.EncodeToString(s.conf.Range.StartKey), hex.EncodeToString(s.conf.Range.EndKey)))
			continue
		}

		l := region.GetLeader()
		if l != nil && region.GetDownPeer(l.Id) == nil {
			haveLeaderCount = haveLeaderCount + 1
			storeid := l.StoreId
			store_leadernum[storeid] = store_leadernum[storeid] + 1
		}
	}
	if haveLeaderCount == 0 {
		schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
		return nil
	}

	targetstore := targetStore(&store_leadernum)

	ops := make([]*operator.Operator, 0)
	for _, region := range regions {
		l := region.GetLeader()
		if l == nil {
			continue
		}
		if targetstore != l.StoreId {
			op, err := operator.CreateTransferLeaderOperator(
				fmt.Sprintf("from affinity controller, r %d ", region.GetID()), cluster, region,
				l.StoreId, targetstore, operator.OpLeader)
			if err != nil {
				log.Warn(fmt.Sprintf("fail to create transfer leader operator, region:%d from %d to %d ",
					region.GetID(), l.StoreId, targetstore), errs.ZapError(err))
				return nil
			}
			op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
			ops = append(ops, op)
		}

	}

	return ops

}

func (l *affinityScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	router := mux.NewRouter()
	router.HandleFunc("/list", l.handleGetConfig).Methods("GET")
	router.HandleFunc("/config", l.handleSetConfig).Methods("POST")
	router.ServeHTTP(w, r)
}

func (l *affinityScheduler) handleGetConfig(w http.ResponseWriter, r *http.Request) {

	rd := render.New(render.Options{IndentJSON: true})
	rd.JSON(w, http.StatusOK, l.conf)
}

func (l *affinityScheduler) handleSetConfig(w http.ResponseWriter, r *http.Request) {

	rd := render.New(render.Options{IndentJSON: true})

	data, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	m := make(map[string]interface{})
	if err := json.Unmarshal(data, &m); err != nil {
		rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	if v, ok := m["range"]; ok {

		if err := json.Unmarshal([]byte(v.(string)), &l.conf.Range); err != nil {
			rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}

		if err := l.persist(); err != nil {
			rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		rd.Text(w, http.StatusOK, "success")
		return
	}

	rd.Text(w, http.StatusBadRequest, "config item not found")
}

func (l *affinityScheduler) persist() error {
	data, err := schedule.EncodeConfig(l.conf)
	if err != nil {
		return err

	}
	return l.storage.SaveScheduleConfig(l.conf.name, data)

}
