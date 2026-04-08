package market

// Parser converts raw exchange messages into canonical structures
type Parser interface {
	ParseTrade(msg []byte) (*Trade, error)
	ParseOrderBook(msg []byte) (*OrderBookUpdate, error)
}
