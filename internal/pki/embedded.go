package pki

import _ "embed"

//go:embed embedded/ca.crt
var CACert []byte

//go:embed embedded/ca.key
var CAKey []byte

//go:embed embedded/client.crt
var ClientCert []byte

//go:embed embedded/client-pk8.key
var ClientKey []byte

//go:embed embedded/broker.crt
var BrokerCert []byte

//go:embed embedded/broker-pk8.key
var BrokerKey []byte
