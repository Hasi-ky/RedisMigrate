package common

import (
	"strconv"
)

func ByteToCID(args []byte) CID {
	return CID(byte(args[0]))
}

func ByteToAddr(args []byte) DevAddr {
	return [4]byte{args[0], args[1], args[2], args[3]}
}

func ByteToEUI(args []byte) EUI64 {
	return [8]byte{args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]}
}

func ByteToAes(args []byte) AES128Key {
	return [16]byte{args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7],
		args[8], args[9], args[10], args[11], args[12], args[13], args[14], args[15]}
}

func StringToDevNounce(args string) DevNonce {
	tempNum, _ := strconv.Atoi(args)
	return DevNonce(tempNum)
}