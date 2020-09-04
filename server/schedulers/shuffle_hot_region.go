// Copyright 2018 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
)

const (
	// ShuffleHotRegionName is shuffle hot region scheduler name.
	ShuffleHotRegionName = "shuffle-hot-region-scheduler"
	// ShuffleHotRegionType is shuffle hot region scheduler type.
	ShuffleHotRegionType = "shuffle-hot-region"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(ShuffleHotRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*shuffleHotRegionSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			conf.Limit = uint64(1)
			if len(args) == 1 {
				limit, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
				}
				conf.Limit = limit
			}
			conf.Name = ShuffleHotRegionName
			return nil
		}
	})

	schedule.RegisterScheduler(ShuffleHotRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleHotRegionSchedulerConfig{Limit: uint64(1)}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleHotRegionScheduler(opController, conf), nil
	})
}

type shuffleHotRegionSchedulerConfig struct {
	Name  string `json:"name"`
	Limit uint64 `json:"limit"`
}

// ShuffleHotRegionScheduler mainly used to test.
// It will randomly pick a hot peer, and move the peer
// to a random store, and then transfer the leader to
// the hot peer.
type shuffleHotRegionScheduler struct {
	*BaseScheduler
	stLoadInfos [resourceTypeLen]map[uint64]*storeLoadDetail
	r           *rand.Rand
	conf        *shuffleHotRegionSchedulerConfig
	types       []rwType
}

// newShuffleHotRegionScheduler creates an admin scheduler that random balance hot regions
func newShuffleHotRegionScheduler(opController *schedule.OperatorController, conf *shuffleHotRegionSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	ret := &shuffleHotRegionScheduler{
		BaseScheduler: base,
		conf:          conf,
		types:         []rwType{read, write},
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.stLoadInfos[ty] = map[uint64]*storeLoadDetail{}
	}
	return ret
}

func (s *shuffleHotRegionScheduler) GetName() string {
	return s.conf.Name
}

func (s *shuffleHotRegionScheduler) GetType() string {
	return ShuffleHotRegionType
}

func (s *shuffleHotRegionScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *shuffleHotRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.OpController.OperatorCount(operator.OpHotRegion) < s.conf.Limit &&
		s.OpController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit() &&
		s.OpController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *shuffleHotRegionScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	i := s.r.Int() % len(s.types)
	return s.dispatch(s.types[i], cluster)
}

func (s *shuffleHotRegionScheduler) dispatch(typ rwType, cluster opt.Cluster) []*operator.Operator {
	storesStats := cluster.GetStoresStats()
	minHotDegree := cluster.GetHotRegionCacheHitsThreshold()
	switch typ {
	case read:
		hotRegionThreshold := getHotRegionThreshold(storesStats, read)
		s.stLoadInfos[readLeader] = summaryStoresLoad(
			storesStats.GetStoresBytesReadStat(),
			storesStats.GetStoresKeysReadStat(),
			map[uint64]Influence{},
			cluster.RegionReadStats(),
			minHotDegree,
			hotRegionThreshold,
			read, core.LeaderKind, mixed)
		return s.randomSchedule(cluster, s.stLoadInfos[readLeader])
	case write:
		hotRegionThreshold := getHotRegionThreshold(storesStats, write)
		s.stLoadInfos[writeLeader] = summaryStoresLoad(
			storesStats.GetStoresBytesWriteStat(),
			storesStats.GetStoresKeysWriteStat(),
			map[uint64]Influence{},
			cluster.RegionWriteStats(),
			minHotDegree,
			hotRegionThreshold,
			write, core.LeaderKind, mixed)
		return s.randomSchedule(cluster, s.stLoadInfos[writeLeader])
	}
	return nil
}

func (s *shuffleHotRegionScheduler) randomSchedule(cluster opt.Cluster, loadDetail map[uint64]*storeLoadDetail) []*operator.Operator {
	for _, detail := range loadDetail {
		if len(detail.HotPeers) < 1 {
			continue
		}
		i := s.r.Intn(len(detail.HotPeers))
		r := detail.HotPeers[i]
		// select src region
		srcRegion := cluster.GetRegion(r.RegionID)
		if srcRegion == nil || len(srcRegion.GetDownPeers()) != 0 || len(srcRegion.GetPendingPeers()) != 0 {
			continue
		}
		srcStoreID := srcRegion.GetLeader().GetStoreId()
		srcStore := cluster.GetStore(srcStoreID)
		if srcStore == nil {
			log.Error("failed to get the source store", zap.Uint64("store-id", srcStoreID), errs.ZapError(errs.ErrGetSourceStore))
		}

		var scoreGuard filter.Filter
		if cluster.IsPlacementRulesEnabled() {
			scoreGuard = filter.NewRuleFitFilter(s.GetName(), cluster, srcRegion, srcStoreID)
		} else {
			scoreGuard = filter.NewDistinctScoreFilter(s.GetName(), cluster.GetLocationLabels(), cluster.GetRegionStores(srcRegion), srcStore)
		}

		filters := []filter.Filter{
			filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true},
			filter.NewExcludedFilter(s.GetName(), srcRegion.GetStoreIds(), srcRegion.GetStoreIds()),
			scoreGuard,
		}
		stores := cluster.GetStores()
		destStoreIDs := make([]uint64, 0, len(stores))
		for _, store := range stores {
			if !filter.Target(cluster, store, filters) {
				continue
			}
			destStoreIDs = append(destStoreIDs, store.GetID())
		}
		if len(destStoreIDs) == 0 {
			return nil
		}
		// random pick a dest store
		destStoreID := destStoreIDs[s.r.Intn(len(destStoreIDs))]
		if destStoreID == 0 {
			return nil
		}
		srcPeer := srcRegion.GetStorePeer(srcStoreID)
		if srcPeer == nil {
			return nil
		}
		destPeer := &metapb.Peer{StoreId: destStoreID}
		op, err := operator.CreateMoveLeaderOperator("random-move-hot-leader", cluster, srcRegion, operator.OpRegion|operator.OpLeader, srcStoreID, destPeer)
		if err != nil {
			log.Debug("fail to create move leader operator", errs.ZapError(err))
			return nil
		}
		op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
		return []*operator.Operator{op}
	}
	schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
	return nil
}
