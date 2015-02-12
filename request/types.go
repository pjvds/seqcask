package request

const (
	T_Append = byte(iota)
)

func NewAppendBody(topic string, partition uint32, bodies [][]byte) AppendBody {
	return AppendBody{
		Topic:     &topic,
		Partition: &partition,
		Messages:  bodies,
	}
}
