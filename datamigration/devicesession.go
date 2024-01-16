package main

import (
	"encoding/hex"
	"time"

	"github.com/gofrs/uuid"
)

// UplinkHistorySize contains the number of frames to store
const UplinkHistorySize = 20

// RXWindow defines the RX window option.
type RXWindow int8
type Array []int

type Channel struct {
	Frequency int
	MinDR     int
	MaxDR     int
	enabled   bool
	custom    bool
}

type DeviceMode string

type CID byte
type DevAddr [4]byte
type EUI64 [8]byte
type AES128Key [16]byte

func (e EUI64) String() string {
	return hex.EncodeToString(e[:])
}

func (a Array) Value() ([]byte, error) {
	out := make([]byte, len(a))
	for i, val := range a {
		out[i] = byte(val)
	}
	return out, nil
}

// Available RX window options.
const (
	RX1 = iota
	RX2
)






// DeviceGatewayRXInfoSet contains the rx-info set of the receiving gateways
// for the last uplink.
type DeviceGatewayRXInfoSet struct {
	DevEUI EUI64
	DR     int
	Items  []DeviceGatewayRXInfo
}

// DeviceGatewayRXInfo holds the meta-data of a gateway receiving the last
// uplink message.
type DeviceGatewayRXInfo struct {
	GatewayID EUI64
	RSSI      int
	LoRaSNR   float64
	Antenna   uint32
	Board     uint32
	Context   []byte
	RfChain   uint32
}

// UplinkHistory contains the meta-data of an uplink transmission.
type UplinkHistory struct {
	FCnt         uint32
	MaxSNR       float64
	TXPowerIndex int
	GatewayCount int
}

// KeyEnvelope defined a key-envelope.
type KeyEnvelope struct {
	KEKLabel string
	AESKey   []byte
}

type DeviceSession struct {
	// MAC version
	MACVersion string `db:"mac_version"`
	// 新增 参数
	BandName   string    `db:"band_name"`
	Nation     string    `db:"nation"`
	ChMask     []byte    `db:"ch_mask"`
	ChGroup    int       `db:"ch_group"`
	Frequency  uint32    `db:"frequency"`
	RmFlag     bool      `db:"rm_flag"`
	UpdateTime time.Time `db:"update_time"`
	// profile ids
	DeviceProfileID  uuid.UUID `db:"device_profile_id"`
	ServiceProfileID uuid.UUID `db:"-"`
	RoutingProfileID uuid.UUID `db:"-"`
	UserId           uuid.UUID `db:"user_id"`
	// session data
	DevAddr        DevAddr      `db:"dev_addr"`
	DevEUI         EUI64        `db:"dev_eui"`
	JoinEUI        EUI64        `db:"join_eui"`
	FNwkSIntKey    AES128Key    `db:"f_nwk_s_int_key"`
	SNwkSIntKey    AES128Key    `db:"s_nwk_s_int_key"`
	NwkSEncKey     AES128Key    `db:"nwk_s_enc_key"`
	AppSKeyEvelope *KeyEnvelope // 被下方AppSKey替代
	AppSKey        AES128Key    `db:"app_s_key"`
	FCntUp         uint32       `db:"f_cnt_up"`
	NFCntDown      uint32       `db:"n_f_cnt_down"`
	AFCntDown      uint32       `db:"a_f_cnt_down"`
	// 全称 confirmCount   标志下行时comfirmmed的个数
	ConfFCnt uint32 `db:"conf_f_cnt"`

	// Only used by ABP activation
	SkipFCntValidation bool `db:"skip_f_cnt_validation"`

	RXWindow     RXWindow `db:"rx_window"`
	RXDelay      uint8    `db:"rx_delay"`
	RX1DROffset  uint8    `db:"rx1_dr_offset"`
	RX2DR        uint8    `db:"rx2_dr"`
	RX2Frequency int      `db:"rx2_frequency"`

	// TXPowerIndex which the node is using. The possible values are defined
	// by the lorawan/band package and are region specific. By default it is
	// assumed that the node is using TXPower 0. This value is controlled by
	// the ADR engine.
	TXPowerIndex int `db:"tx_power_index"`

	// DR defines the (last known) data-rate at which the node is operating.
	// This value is controlled by the ADR engine.
	DR int `db:"dr"`

	// ADR defines if the device has ADR enabled.
	ADR bool `db:"adr"`

	// MinSupportedTXPowerIndex defines the minimum supported tx-power index
	// by the node (default 0).
	MinSupportedTXPowerIndex int `db:"min_supported_tx_power_index"`

	// MaxSupportedTXPowerIndex defines the maximum supported tx-power index
	// by the node, or 0 when not set.
	MaxSupportedTXPowerIndex int `db:"max_supported_tx_power_index"`

	// NbTrans defines the number of transmissions for each unconfirmed uplink
	// frame. In case of 0, the default value is used.
	// This value is controlled by the ADR engine.
	NbTrans               uint8           `db:"nb_trans"`
	EnabledChannels       []int           `db:"enabled_channels"`        // deprecated, migrated by GetDeviceSession
	EnabledUplinkChannels []int           `db:"enabled_uplink_channels"` // channels that are activated on the node
	ExtraUplinkChannels   map[int]Channel `db:"-"`                       // extra uplink channels, configured by the user
	ChannelFrequencies    []int           `db:"channel_frequencies"`     // frequency of each channel
	UplinkHistory         []UplinkHistory `db:"-"`                       // contains the last 20 transmissions

	// LastDevStatusRequest contains the timestamp when the last device-status
	// request was made.
	LastDevStatusRequested time.Time `db:"last_dev_status_requested"`

	// LastDownlinkTX contains the timestamp of the last downlink.
	LastDownlinkTX time.Time `db:"last_downlink_tx"`

	// Class-B related configuration.
	BeaconLocked      bool `db:"beacon_locked"`
	PingSlotNb        int  `db:"ping_slot_nb"`
	PingSlotDR        int  `db:"ping_slot_dr"`
	PingSlotFrequency int  `db:"pint_slot_frequency"`

	// RejoinRequestEnabled defines if the rejoin-request is enabled on the
	// device.
	RejoinRequestEnabled bool `db:"rejoin_request_enabled"`

	// RejoinRequestMaxCountN defines the 2^(C+4) uplink message interval for
	// the rejoin-request.
	RejoinRequestMaxCountN int `db:"rejoin_request_max_count_n"`

	// RejoinRequestMaxTimeN defines the 2^(T+10) time interval (seconds)
	// for the rejoin-request.
	RejoinRequestMaxTimeN int `db:"rejoin_request_max_time_n"`

	RejoinCount0               uint16         `db:"rejoin_count0"`
	PendingRejoinDeviceSession *DeviceSession `db:"-"`

	// ReferenceAltitude holds the device reference altitude used for
	// geolocation.
	ReferenceAltitude float64 `db:"reference_altitude"`

	// Uplink and Downlink dwell time limitations.
	UplinkDwellTime400ms   bool `db:"uplink_dwell_time_400_s"`
	DownlinkDwellTime400ms bool `db:"downlink_dwell_time_400_s"`

	// Max uplink EIRP limitation.
	UplinkMaxEIRPIndex uint8 `db:"uplink_max_erip_index"`

	// Delayed mac-commands.
	MACCommandErrorCount map[CID]int `db:"-"`

	// Device is disabled.
	IsDisabled      bool       `db:"is_disabled"`
	FPortUp         uint8      `db:"f_port_up"`
	DeviceMode      DeviceMode `db:"device_mode"`
	DevType         string     `db:"dev_type" json:"devType"`
	AlivePktType    int        `db:"alive_pkt_type" json:"alivePktType"`
	KeepalivePeriod int        `db:"alive_period" json:"alivePeriod"`
	Debug           bool       `db:"debug" json:"debug"`
	DevName         string     `db:"dev_name" json:"devName"`
}





func deviceSessionToPB(d DeviceSession) *DeviceSessionPB {
	out := DeviceSessionPB{
		// 新增参数
		BandName:   d.BandName,
		Nation:     d.Nation,
		ChGroup:    uint32(d.ChGroup),
		ChMask:     d.ChMask[:],
		Frequency:  d.Frequency,
		RmFlag:     d.RmFlag,
		UpdateTime: d.UpdateTime.UnixNano(),

		MacVersion:       d.MACVersion,
		DeviceProfileId:  d.DeviceProfileID.String(),
		ServiceProfileId: d.ServiceProfileID.String(),
		RoutingProfileId: d.RoutingProfileID.String(),
		UserId:           d.UserId.String(),
		DevAddr:          d.DevAddr[:],
		DevEui:           d.DevEUI[:],
		JoinEui:          d.JoinEUI[:],
		FNwkSIntKey:      d.FNwkSIntKey[:],
		SNwkSIntKey:      d.SNwkSIntKey[:],
		NwkSEncKey:       d.NwkSEncKey[:],
		AppSKey:          d.AppSKey[:],
		FPortUp:          uint32(d.FPortUp),
		FCntUp:           d.FCntUp,
		NFCntDown:        d.NFCntDown,
		AFCntDown:        d.AFCntDown,
		ConfFCnt:         d.ConfFCnt,
		SkipFCntCheck:    d.SkipFCntValidation,

		RxDelay:      uint32(d.RXDelay),
		Rx1DrOffset:  uint32(d.RX1DROffset),
		Rx2Dr:        uint32(d.RX2DR),
		Rx2Frequency: uint32(d.RX2Frequency),
		TxPowerIndex: uint32(d.TXPowerIndex),

		Dr:                       uint32(d.DR),
		Adr:                      d.ADR,
		MinSupportedTxPowerIndex: uint32(d.MinSupportedTXPowerIndex),
		MaxSupportedTxPowerIndex: uint32(d.MaxSupportedTXPowerIndex),
		NbTrans:                  uint32(d.NbTrans),

		ExtraUplinkChannels: make(map[uint32]*DeviceSessionPBChannel),

		LastDeviceStatusRequestTimeUnixNs: d.LastDevStatusRequested.UnixNano(),

		LastDownlinkTxTimestampUnixNs: d.LastDownlinkTX.UnixNano(),
		BeaconLocked:                  d.BeaconLocked,
		PingSlotNb:                    uint32(d.PingSlotNb),
		PingSlotDr:                    uint32(d.PingSlotDR),
		PingSlotFrequency:             uint32(d.PingSlotFrequency),

		RejoinRequestEnabled:   d.RejoinRequestEnabled,
		RejoinRequestMaxCountN: uint32(d.RejoinRequestMaxCountN),
		RejoinRequestMaxTimeN:  uint32(d.RejoinRequestMaxTimeN),

		RejoinCount_0:     uint32(d.RejoinCount0),
		ReferenceAltitude: d.ReferenceAltitude,

		UplinkDwellTime_400Ms:   d.UplinkDwellTime400ms,
		DownlinkDwellTime_400Ms: d.DownlinkDwellTime400ms,
		UplinkMaxEirpIndex:      uint32(d.UplinkMaxEIRPIndex),

		MacCommandErrorCount: make(map[uint32]uint32),
		IsDisabled:           d.IsDisabled,
		DeviceMode:           string(d.DeviceMode),
		DevType:              d.DevType,
		AlivePktType:         uint32(d.AlivePktType),
		AlivePeriod:          uint64(d.KeepalivePeriod),
		Debug:                d.Debug,
		DevName:              d.DevName,
	}

	//if d.AppSKeyEvelope != nil {
	//	out.AppSKeyEnvelope = &common.KeyEnvelope{
	//		KekLabel: d.AppSKeyEvelope.KEKLabel,
	//		AesKey:   d.AppSKeyEvelope.AESKey,
	//	}
	//}

	for _, c := range d.EnabledUplinkChannels {
		out.EnabledUplinkChannels = append(out.EnabledUplinkChannels, uint32(c))
	}

	for i, c := range d.ExtraUplinkChannels {
		out.ExtraUplinkChannels[uint32(i)] = &DeviceSessionPBChannel{
			Frequency: uint32(c.Frequency),
			MinDr:     uint32(c.MinDR),
			MaxDr:     uint32(c.MaxDR),
		}
	}

	for _, c := range d.ChannelFrequencies {
		out.ChannelFrequencies = append(out.ChannelFrequencies, uint32(c))
	}

	//for _, h := range d.UplinkHistory {
	//	out.UplinkAdrHistory = append(out.UplinkAdrHistory, &DeviceSessionPBUplinkADRHistory{
	//		FCnt:         h.FCnt,
	//		MaxSnr:       float32(h.MaxSNR),
	//		TxPowerIndex: uint32(h.TXPowerIndex),
	//		GatewayCount: uint32(h.GatewayCount),
	//	})
	//}
	for k, v := range d.MACCommandErrorCount {
		out.MacCommandErrorCount[uint32(k)] = uint32(v)
	}

	return &out
}



func deviceGatewayRXInfoSetToPB(d DeviceGatewayRXInfoSet) *DeviceGatewayRXInfoSetPB {
	out := DeviceGatewayRXInfoSetPB{
		DevEui: d.DevEUI[:],
		Dr:     uint32(d.DR),
	}

	for i := range d.Items {
		out.Items = append(out.Items, &DeviceGatewayRXInfoPB{
			GatewayId: d.Items[i].GatewayID[:],
			Rssi:      int32(d.Items[i].RSSI),
			LoraSnr:   d.Items[i].LoRaSNR,
			Board:     d.Items[i].Board,
			Antenna:   d.Items[i].Antenna,
			Context:   d.Items[i].Context,
		})
	}

	return &out
}



 