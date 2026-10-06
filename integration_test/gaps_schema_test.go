//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	storepostgres "github.com/eventsalsa/store/postgres"

	projectorpkg "github.com/eventsalsa/projector"
	projectormigrations "github.com/eventsalsa/projector/migrations"
	projectorpostgres "github.com/eventsalsa/projector/postgres"
)

const infraSchema = "infra"

func generateProjectorSQLForSchema(t testing.TB, outputDir, schema string) []byte {
	t.Helper()

	config := &projectormigrations.Config{
		OutputFolder:               outputDir,
		OutputFilename:             "projector_" + schema + ".sql",
		ProjectorInstancesTable:    schema + ".projector_instances",
		ProjectionAssignmentsTable: schema + ".projection_assignments",
		ProjectionCheckpointsTable: schema + ".projection_checkpoints",
		ProjectionGapSkipsTable:    schema + ".projection_gap_skips",
		ProjectorLeaderLeasesTable: schema + ".projector_leader_leases",
	}
	if err := projectormigrations.GeneratePostgres(config); err != nil {
		t.Fatalf("generate %s projector migration: %v", schema, err)
	}

	sqlBytes, err := os.ReadFile(filepath.Join(outputDir, config.OutputFilename))
	if err != nil {
		t.Fatalf("read %s projector migration: %v", schema, err)
	}

	return sqlBytes
}

func TestGap_SchemaQualifiedProjectorTables(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	ctx := context.Background()
	if _, err := controlDB.Exec(ctx, `DROP SCHEMA IF EXISTS `+infraSchema+` CASCADE`); err != nil {
		t.Fatalf("drop %s schema: %v", infraSchema, err)
	}

	sqlBytes := generateProjectorSQLForSchema(t, t.TempDir(), infraSchema)
	if _, err := controlDB.Exec(ctx, string(sqlBytes)); err != nil {
		t.Fatalf("execute %s projector migration: %v", infraSchema, err)
	}

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("infra-projection", "projector-1", nil)

	options := append(defaultProjectorOptions(),
		projectorpkg.WithProjectorInstancesTable(infraSchema+".projector_instances"),
		projectorpkg.WithProjectionAssignmentsTable(infraSchema+".projection_assignments"),
		projectorpkg.WithProjectionCheckpointsTable(infraSchema+".projection_checkpoints"),
		projectorpkg.WithProjectionGapSkipsTable(infraSchema+".projection_gap_skips"),
		projectorpkg.WithProjectorLeaderLeasesTable(infraSchema+".projector_leader_leases"),
	)

	harness := startTestProjectorWithTables(t, "projector-1",
		[]projectorpkg.Projection{projection},
		infraSchema+".projector_instances",
		options...,
	)

	appended := appendTestEvents(t, controlDB, eventStore, 2, "Order")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		checkpoint, err := projectorpostgres.GetCheckpoint(ctx, controlDB, infraSchema+".projection_checkpoints", projection.Name())
		if err != nil {
			return err
		}
		if checkpoint != latest {
			return fmt.Errorf("%s checkpoint=%d want %d", infraSchema, checkpoint, latest)
		}
		return nil
	})

	var defaultRows int
	if err := controlDB.QueryRow(ctx, `SELECT COUNT(*) FROM projector_instances`).Scan(&defaultRows); err != nil {
		t.Fatalf("count default projector_instances: %v", err)
	}
	if defaultRows != 0 {
		t.Fatalf("default projector_instances has %d rows, want 0 for a schema-qualified daemon", defaultRows)
	}

	harness.stop(t)
}
