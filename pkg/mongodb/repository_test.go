package mongodb_test

import (
	"testing"

	"github.com/google/uuid"
	shared "github.com/resource-ownership/go-common/pkg/common"
	"github.com/resource-ownership/go-mongodb/pkg/mongodb"
	"github.com/stretchr/testify/assert"
)

// MockEntity is a test entity for testing the generic repository
type MockEntity struct {
	ID            uuid.UUID            `bson:"_id"`
	Name          string               `bson:"name"`
	ResourceOwner shared.ResourceOwner `bson:"resource_owner"`
	BaseEntity    shared.BaseEntity    `bson:"baseentity"`
}

func (e MockEntity) GetID() uuid.UUID {
	return e.ID
}

func TestMongoDBRepository_GetBSONFieldName(t *testing.T) {
	// This is a basic test to ensure the repository can be instantiated
	// and field mapping works correctly
	repo := mongodb.NewMongoDBRepository[MockEntity](nil, "test", MockEntity{}, "test_collection", "MockEntity")

	// Initialize field mappings manually for testing
	repo.BsonFieldMappings = map[string]string{
		"ID":   "_id",
		"Name": "name",
	}

	fieldName, err := repo.GetBSONFieldName("Name")
	assert.NoError(t, err)
	assert.Equal(t, "name", fieldName)

	fieldName, err = repo.GetBSONFieldName("ID")
	assert.NoError(t, err)
	assert.Equal(t, "_id", fieldName)
}