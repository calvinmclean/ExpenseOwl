package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/tanq16/expenseowl/internal/storage"
)

func main() {
	var sourceConfigJSON, destConfigJSON string
	flag.StringVar(&sourceConfigJSON, "source", "", "Source storage SystemConfig JSON")
	flag.StringVar(&destConfigJSON, "dest", "", "Destination storage SystemConfig JSON")
	flag.Parse()

	if sourceConfigJSON == "" || destConfigJSON == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -source '<json>' -dest '<json>'\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s -source '{\"StorageType\":\"json\",\"StorageURL\":\"/path/to/source\"}' -dest '{\"StorageType\":\"postgres\",\"StorageURL\":\"localhost:5432/expenses\",\"StorageUser\":\"user\",\"StoragePass\":\"pass\"}'\n", os.Args[0])
		os.Exit(1)
	}

	var sourceConfig, destConfig storage.SystemConfig

	if err := json.Unmarshal([]byte(sourceConfigJSON), &sourceConfig); err != nil {
		log.Fatalf("Failed to parse source config: %v", err)
	}

	if err := json.Unmarshal([]byte(destConfigJSON), &destConfig); err != nil {
		log.Fatalf("Failed to parse destination config: %v", err)
	}

	sourceStore, err := initializeStore(sourceConfig)
	if err != nil {
		log.Fatalf("Failed to initialize source storage: %v", err)
	}
	defer sourceStore.Close()

	destStore, err := initializeStore(destConfig)
	if err != nil {
		log.Fatalf("Failed to initialize destination storage: %v", err)
	}
	defer destStore.Close()

	log.Println("Reading data from source...")

	sourceDataConfig, err := sourceStore.GetConfig()
	if err != nil {
		log.Fatalf("Failed to get source config: %v", err)
	}

	sourceExpenses, err := sourceStore.GetAllExpenses()
	if err != nil {
		log.Fatalf("Failed to get source expenses: %v", err)
	}

	log.Printf("Found %d categories, %d recurring expenses, %d expenses in source",
		len(sourceDataConfig.Categories),
		len(sourceDataConfig.RecurringExpenses),
		len(sourceExpenses))

	log.Println("Writing data to destination...")

	if err := destStore.UpdateCategories(sourceDataConfig.Categories); err != nil {
		log.Fatalf("Failed to migrate categories: %v", err)
	}
	log.Printf("Migrated %d categories", len(sourceDataConfig.Categories))

	if err := destStore.UpdateCurrency(sourceDataConfig.Currency); err != nil {
		log.Fatalf("Failed to migrate currency: %v", err)
	}
	log.Printf("Migrated currency: %s", sourceDataConfig.Currency)

	if err := destStore.UpdateStartDate(sourceDataConfig.StartDate); err != nil {
		log.Fatalf("Failed to migrate start date: %v", err)
	}
	log.Printf("Migrated start date: %d", sourceDataConfig.StartDate)

	for _, recExp := range sourceDataConfig.RecurringExpenses {
		if err := destStore.AddRecurringExpense(recExp); err != nil {
			log.Fatalf("Failed to migrate recurring expense %s: %v", recExp.ID, err)
		}
	}
	log.Printf("Migrated %d recurring expenses", len(sourceDataConfig.RecurringExpenses))

	if len(sourceExpenses) > 0 {
		if err := destStore.AddMultipleExpenses(sourceExpenses); err != nil {
			log.Fatalf("Failed to migrate expenses: %v", err)
		}
		log.Printf("Migrated %d expenses", len(sourceExpenses))
	}

	log.Println("Migration completed successfully!")
}

func initializeStore(config storage.SystemConfig) (storage.Storage, error) {
	switch config.StorageType {
	case storage.BackendTypeJSON:
		return storage.InitializeJsonStore(config)
	case storage.BackendTypePostgres:
		return storage.InitializePostgresStore(config)
	case storage.BackendTypeSQLite:
		return storage.InitializeSQLiteStore(config)
	default:
		return nil, fmt.Errorf("invalid storage type: %s", config.StorageType)
	}
}
