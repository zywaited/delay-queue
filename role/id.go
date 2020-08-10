package role

type GenerateId func() (string, string)

type GenerateIdType string

var generateIds GenerateId

func AcGenerateId() GenerateId {
	return generateIds
}

func RegisterGenerateId(gi GenerateId) {
	generateIds = gi
}
