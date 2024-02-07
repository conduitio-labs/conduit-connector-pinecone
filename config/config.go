package config

// Config contains shared config parameters, common to the source and
// destination. If you don't need shared parameters you can entirely remove this
// file.

const (
	// ConfigPineconeAPIKey is the config name for Pinecone API key
	ConfigPineconeAPIKey = "pinecone.apiKey"

	// ConfigPineconeHostURL is the config name for Pinecone Host URL
	ConfigPineconeHostURL = "pinecone.hostURL"

	// ConfigOpenAIKey is the config name for OpenAI API Key
	ConfigOpenAIKey = "pinecone.openAIKey"
)

// Config represents configuration needed for Pinecone
type Config struct {
	// Pinecone API key.
	PineconeAPIKey string `json:"pinecone.apiKey" validate:"required"`
	// Host URL for Pinecone index.
	PineconeHostURL string `json:"pinecone.hostURL" validate:"required"`
	// OpenAI API key for embedding vectors
	OpenAIKey string `json:"pinecone.openAIKey" validate:"required"`
}
