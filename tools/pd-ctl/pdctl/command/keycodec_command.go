package command

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
	"github.com/tikv/pd/pkg/codec"
)

// NewLabelCommand return a member subcommand of rootCmd
func NewKeycodecCommand() *cobra.Command {
	l := &cobra.Command{
		Use:   "keycodec",
		Short: "encode/decode the key",
	}
	l.AddCommand(NewEncodeCommand())
	l.AddCommand(NewDecodeCommand())
	return l
}

// NewLabelListStoresCommand return a label subcommand of labelCmd
func NewEncodeCommand() *cobra.Command {
	l := &cobra.Command{
		Use:   "encode  <key> [from_hex]",
		Short: "encode",
		Run:   encode,
	}
	l.Flags().BoolP("base64", "b", false, "output base64.")
	l.Flags().BoolP("url", "q", false, "output url.QueryEscape")
	return l
}

// NewLabelListStoresCommand return a label subcommand of labelCmd
func NewDecodeCommand() *cobra.Command {
	l := &cobra.Command{
		Use:   "decode   <key> ",
		Short: "decode",
		Run:   decode,
	}
	l.Flags().BoolP("base64", "b", false, "from base64.")
	return l
}

func encode(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	var err error
	key := []byte(args[0])
	if len(args) > 1 && args[1] == "from_hex" {
		key, err = hex.DecodeString(args[0])
		if err != nil {
			cmd.Printf("Failed to hex.DecodeString: %s\n", err)
			return
		}
	}
	ekey := codec.EncodeBytes([]byte(key))
	if v, _ := cmd.Flags().GetBool("base64"); v {
		cmd.Println(fmt.Sprintf("%s", base64.StdEncoding.EncodeToString(ekey)))
	} else if v, _ := cmd.Flags().GetBool("url"); v {
		cmd.Println(fmt.Sprintf("%s", url.QueryEscape(string(ekey))))
	} else {
		cmd.Println(fmt.Sprintf("%X", ekey))
	}
}

func decode(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	var err error
	var bs []byte
	if v, _ := cmd.Flags().GetBool("base64"); v {
		bs, err = base64.StdEncoding.DecodeString(args[0])
		if err != nil {
			cmd.Printf("Failed to base64 decode: %s\n", err)
			return
		}
		cmd.Println(fmt.Sprintf("hex:%X", string(bs)))
	} else {
		bs, err = hex.DecodeString(args[0])
		if err != nil {
			cmd.Printf("Failed to hex decode: %s\n", err)
			return
		}
	}

	l, key, err := codec.DecodeBytes(bs)
	if err != nil {
		cmd.Printf("Failed to DecodeBytes: %s\n", err)
		return
	}

	cmd.Println(fmt.Sprintf("key:%s", string(key)))
	cmd.Println(fmt.Sprintf("left:%X", l))
}
