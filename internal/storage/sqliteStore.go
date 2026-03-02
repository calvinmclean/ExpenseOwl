package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

// sqliteStore implements the Storage interface for SQLite.
type sqliteStore struct {
	db       *sql.DB
	defaults map[string]string // allows reusing defaults without querying for config
}

// SQL queries as constants for reusability and clarity.
const (
	createExpensesTableSQLiteSQL = `
	CREATE TABLE IF NOT EXISTS expenses (
		id VARCHAR(36) PRIMARY KEY,
		recurring_id VARCHAR(36),
		name VARCHAR(255) NOT NULL,
		category VARCHAR(255) NOT NULL,
		amount REAL NOT NULL,
		currency VARCHAR(3) NOT NULL,
		date DATETIME NOT NULL,
		tags TEXT
	);`

	createRecurringExpensesTableSQLiteSQL = `
	CREATE TABLE IF NOT EXISTS recurring_expenses (
		id VARCHAR(36) PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		amount REAL NOT NULL,
		currency VARCHAR(3) NOT NULL,
		category VARCHAR(255) NOT NULL,
		start_date DATETIME NOT NULL,
		interval VARCHAR(50) NOT NULL,
		occurrences INTEGER NOT NULL,
		tags TEXT
	);`

	createConfigTableSQLiteSQL = `
	CREATE TABLE IF NOT EXISTS config (
		id VARCHAR(255) PRIMARY KEY DEFAULT 'default',
		categories TEXT NOT NULL,
		currency VARCHAR(255) NOT NULL,
		start_date INTEGER NOT NULL
	);`
)

func InitializeSQLiteStore(baseConfig SystemConfig) (Storage, error) {
	dbPath := baseConfig.StorageURL
	if !strings.HasSuffix(dbPath, ".db") {
		dbPath = filepath.Join(dbPath, "expenseowl.db")
	}

	// Ensure directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %v", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping SQLite database: %v", err)
	}
	log.Println("Connected to SQLite database")

	if err := createSQLiteTables(db); err != nil {
		return nil, fmt.Errorf("failed to create database tables: %v", err)
	}
	return &sqliteStore{db: db, defaults: map[string]string{}}, nil
}

func createSQLiteTables(db *sql.DB) error {
	for _, query := range []string{createExpensesTableSQLiteSQL, createRecurringExpensesTableSQLiteSQL, createConfigTableSQLiteSQL} {
		if _, err := db.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

func (s *sqliteStore) Close() error {
	return s.db.Close()
}

func (s *sqliteStore) saveConfig(config *Config) error {
	categoriesJSON, err := json.Marshal(config.Categories)
	if err != nil {
		return fmt.Errorf("failed to marshal categories: %v", err)
	}
	query := `
		INSERT INTO config (id, categories, currency, start_date)
		VALUES ('default', ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			categories = excluded.categories,
			currency = excluded.currency,
			start_date = excluded.start_date;
	`
	_, err = s.db.Exec(query, string(categoriesJSON), config.Currency, config.StartDate)
	s.defaults["currency"] = config.Currency
	s.defaults["start_date"] = fmt.Sprintf("%d", config.StartDate)
	return err
}

func (s *sqliteStore) updateConfig(updater func(c *Config) error) error {
	config, err := s.GetConfig()
	if err != nil {
		return err
	}
	if err := updater(config); err != nil {
		return err
	}
	return s.saveConfig(config)
}

func (s *sqliteStore) GetConfig() (*Config, error) {
	query := `SELECT categories, currency, start_date FROM config WHERE id = 'default'`
	var categoriesStr, currency string
	var startDate int
	err := s.db.QueryRow(query).Scan(&categoriesStr, &currency, &startDate)

	if err != nil {
		if err == sql.ErrNoRows {
			config := &Config{}
			config.SetBaseConfig()
			if err := s.saveConfig(config); err != nil {
				return nil, fmt.Errorf("failed to save initial default config: %v", err)
			}
			return config, nil
		}
		return nil, fmt.Errorf("failed to get config from db: %v", err)
	}

	var config Config
	config.Currency = currency
	config.StartDate = startDate
	if err := json.Unmarshal([]byte(categoriesStr), &config.Categories); err != nil {
		return nil, fmt.Errorf("failed to parse categories from db: %v", err)
	}

	recurring, err := s.GetRecurringExpenses()
	if err != nil {
		return nil, fmt.Errorf("failed to get recurring expenses for config: %v", err)
	}
	config.RecurringExpenses = recurring

	return &config, nil
}

func (s *sqliteStore) GetCategories() ([]string, error) {
	config, err := s.GetConfig()
	if err != nil {
		return nil, err
	}
	return config.Categories, nil
}

func (s *sqliteStore) UpdateCategories(categories []string) error {
	return s.updateConfig(func(c *Config) error {
		c.Categories = categories
		return nil
	})
}

func (s *sqliteStore) GetCurrency() (string, error) {
	config, err := s.GetConfig()
	if err != nil {
		return "", err
	}
	return config.Currency, nil
}

func (s *sqliteStore) UpdateCurrency(currency string) error {
	if !slices.Contains(SupportedCurrencies, currency) {
		return fmt.Errorf("invalid currency: %s", currency)
	}
	return s.updateConfig(func(c *Config) error {
		c.Currency = currency
		return nil
	})
}

func (s *sqliteStore) GetStartDate() (int, error) {
	config, err := s.GetConfig()
	if err != nil {
		return 0, err
	}
	return config.StartDate, nil
}

func (s *sqliteStore) UpdateStartDate(startDate int) error {
	if startDate < 1 || startDate > 31 {
		return fmt.Errorf("invalid start date: %d", startDate)
	}
	return s.updateConfig(func(c *Config) error {
		c.StartDate = startDate
		return nil
	})
}

func scanSQLiteExpense(scanner interface{ Scan(...any) error }) (Expense, error) {
	var expense Expense
	var tagsStr sql.NullString
	var recurringID sql.NullString
	var dateStr sql.NullString
	err := scanner.Scan(&expense.ID, &recurringID, &expense.Name, &expense.Category, &expense.Amount, &dateStr, &tagsStr)
	if err != nil {
		return Expense{}, err
	}
	if recurringID.Valid {
		expense.RecurringID = recurringID.String
	}
	if dateStr.Valid && dateStr.String != "" {
		parsedDate, err := time.Parse(time.RFC3339, dateStr.String)
		if err != nil {
			return Expense{}, fmt.Errorf("failed to parse date for expense %s: %v", expense.ID, err)
		}
		expense.Date = parsedDate
	}
	if tagsStr.Valid && tagsStr.String != "" {
		if err := json.Unmarshal([]byte(tagsStr.String), &expense.Tags); err != nil {
			return Expense{}, fmt.Errorf("failed to parse tags for expense %s: %v", expense.ID, err)
		}
	}
	return expense, nil
}

func (s *sqliteStore) GetAllExpenses() ([]Expense, error) {
	query := `SELECT id, recurring_id, name, category, amount, date, tags FROM expenses ORDER BY date DESC`
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query expenses: %v", err)
	}
	defer rows.Close()

	var expenses []Expense
	for rows.Next() {
		expense, err := scanSQLiteExpense(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan expense: %v", err)
		}
		expenses = append(expenses, expense)
	}
	return expenses, nil
}

func (s *sqliteStore) GetExpense(id string) (Expense, error) {
	query := `SELECT id, recurring_id, name, category, amount, date, tags FROM expenses WHERE id = ?`
	expense, err := scanSQLiteExpense(s.db.QueryRow(query, id))
	if err != nil {
		if err == sql.ErrNoRows {
			return Expense{}, fmt.Errorf("expense with ID %s not found", id)
		}
		return Expense{}, fmt.Errorf("failed to get expense: %v", err)
	}
	return expense, nil
}

func (s *sqliteStore) AddExpense(expense Expense) error {
	if expense.ID == "" {
		expense.ID = uuid.New().String()
	}
	if expense.Currency == "" {
		expense.Currency = s.defaults["currency"]
	}
	if expense.Date.IsZero() {
		expense.Date = time.Now()
	}
	tagsJSON, err := json.Marshal(expense.Tags)
	if err != nil {
		return err
	}
	query := `
		INSERT INTO expenses (id, recurring_id, name, category, amount, currency, date, tags)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`
	_, err = s.db.Exec(query, expense.ID, expense.RecurringID, expense.Name, expense.Category, expense.Amount, expense.Currency, expense.Date.Format(time.RFC3339), string(tagsJSON))
	return err
}

func (s *sqliteStore) UpdateExpense(id string, expense Expense) error {
	tagsJSON, err := json.Marshal(expense.Tags)
	if err != nil {
		return err
	}
	if expense.Currency == "" {
		expense.Currency = s.defaults["currency"]
	}
	query := `
		UPDATE expenses
		SET name = ?, category = ?, amount = ?, currency = ?, date = ?, tags = ?, recurring_id = ?
		WHERE id = ?
	`
	result, err := s.db.Exec(query, expense.Name, expense.Category, expense.Amount, expense.Currency, expense.Date.Format(time.RFC3339), string(tagsJSON), expense.RecurringID, id)
	if err != nil {
		return fmt.Errorf("failed to update expense: %v", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %v", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("expense with ID %s not found", id)
	}
	return nil
}

func (s *sqliteStore) RemoveExpense(id string) error {
	query := `DELETE FROM expenses WHERE id = ?`
	result, err := s.db.Exec(query, id)
	if err != nil {
		return fmt.Errorf("failed to delete expense: %v", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %v", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("expense with ID %s not found", id)
	}
	return nil
}

func (s *sqliteStore) AddMultipleExpenses(expenses []Expense) error {
	if len(expenses) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO expenses (id, recurring_id, name, category, amount, currency, date, tags)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	for _, expense := range expenses {
		if expense.ID == "" {
			expense.ID = uuid.New().String()
		}
		if expense.Currency == "" {
			expense.Currency = s.defaults["currency"]
		}
		if expense.Date.IsZero() {
			expense.Date = time.Now()
		}
		tagsJSON, _ := json.Marshal(expense.Tags)
		_, err = stmt.Exec(expense.ID, expense.RecurringID, expense.Name, expense.Category, expense.Amount, expense.Currency, expense.Date.Format(time.RFC3339), string(tagsJSON))
		if err != nil {
			return fmt.Errorf("failed to insert expense: %v", err)
		}
	}
	return tx.Commit()
}

func (s *sqliteStore) RemoveMultipleExpenses(ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	query := fmt.Sprintf(`DELETE FROM expenses WHERE id IN (%s)`, strings.Join(placeholders, ","))
	_, err := s.db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to delete multiple expenses: %v", err)
	}
	return nil
}

func scanSQLiteRecurringExpense(scanner interface{ Scan(...any) error }) (RecurringExpense, error) {
	var re RecurringExpense
	var tagsStr sql.NullString
	var startDateStr sql.NullString
	err := scanner.Scan(&re.ID, &re.Name, &re.Amount, &re.Currency, &re.Category, &startDateStr, &re.Interval, &re.Occurrences, &tagsStr)
	if err != nil {
		return RecurringExpense{}, err
	}
	if startDateStr.Valid && startDateStr.String != "" {
		parsedDate, err := time.Parse(time.RFC3339, startDateStr.String)
		if err != nil {
			return RecurringExpense{}, fmt.Errorf("failed to parse start_date for recurring expense %s: %v", re.ID, err)
		}
		re.StartDate = parsedDate
	}
	if tagsStr.Valid && tagsStr.String != "" {
		if err := json.Unmarshal([]byte(tagsStr.String), &re.Tags); err != nil {
			return RecurringExpense{}, fmt.Errorf("failed to parse tags for recurring expense %s: %v", re.ID, err)
		}
	}
	return re, nil
}

func (s *sqliteStore) GetRecurringExpenses() ([]RecurringExpense, error) {
	query := `SELECT id, name, amount, currency, category, start_date, interval, occurrences, tags FROM recurring_expenses`
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query recurring expenses: %v", err)
	}
	defer rows.Close()
	var recurringExpenses []RecurringExpense
	for rows.Next() {
		re, err := scanSQLiteRecurringExpense(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan recurring expense: %v", err)
		}
		recurringExpenses = append(recurringExpenses, re)
	}
	return recurringExpenses, nil
}

func (s *sqliteStore) GetRecurringExpense(id string) (RecurringExpense, error) {
	query := `SELECT id, name, amount, currency, category, start_date, interval, occurrences, tags FROM recurring_expenses WHERE id = ?`
	re, err := scanSQLiteRecurringExpense(s.db.QueryRow(query, id))
	if err != nil {
		if err == sql.ErrNoRows {
			return RecurringExpense{}, fmt.Errorf("recurring expense with ID %s not found", id)
		}
		return RecurringExpense{}, fmt.Errorf("failed to get recurring expense: %v", err)
	}
	return re, nil
}

func (s *sqliteStore) AddRecurringExpense(recurringExpense RecurringExpense) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	if recurringExpense.ID == "" {
		recurringExpense.ID = uuid.New().String()
	}
	if recurringExpense.Currency == "" {
		recurringExpense.Currency = s.defaults["currency"]
	}
	tagsJSON, _ := json.Marshal(recurringExpense.Tags)
	ruleQuery := `
		INSERT INTO recurring_expenses (id, name, amount, currency, category, start_date, interval, occurrences, tags)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	_, err = tx.Exec(ruleQuery, recurringExpense.ID, recurringExpense.Name, recurringExpense.Amount, recurringExpense.Currency, recurringExpense.Category, recurringExpense.StartDate.Format(time.RFC3339), recurringExpense.Interval, recurringExpense.Occurrences, string(tagsJSON))
	if err != nil {
		return fmt.Errorf("failed to insert recurring expense rule: %v", err)
	}

	expensesToAdd := generateExpensesFromRecurring(recurringExpense, false)
	if len(expensesToAdd) > 0 {
		stmt, err := tx.Prepare(`
			INSERT INTO expenses (id, recurring_id, name, category, amount, currency, date, tags)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %v", err)
		}
		defer stmt.Close()
		for _, exp := range expensesToAdd {
			expTagsJSON, _ := json.Marshal(exp.Tags)
			_, err = stmt.Exec(exp.ID, exp.RecurringID, exp.Name, exp.Category, exp.Amount, exp.Currency, exp.Date.Format(time.RFC3339), string(expTagsJSON))
			if err != nil {
				return fmt.Errorf("failed to insert expense: %v", err)
			}
		}
	}
	return tx.Commit()
}

func (s *sqliteStore) UpdateRecurringExpense(id string, recurringExpense RecurringExpense, updateAll bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()
	recurringExpense.ID = id
	if recurringExpense.Currency == "" {
		recurringExpense.Currency = s.defaults["currency"]
	}
	tagsJSON, _ := json.Marshal(recurringExpense.Tags)
	ruleQuery := `
		UPDATE recurring_expenses
		SET name = ?, amount = ?, category = ?, start_date = ?, interval = ?, occurrences = ?, tags = ?, currency = ?
		WHERE id = ?
	`
	res, err := tx.Exec(ruleQuery, recurringExpense.Name, recurringExpense.Amount, recurringExpense.Category, recurringExpense.StartDate.Format(time.RFC3339), recurringExpense.Interval, recurringExpense.Occurrences, string(tagsJSON), recurringExpense.Currency, id)
	if err != nil {
		return fmt.Errorf("failed to update recurring expense rule: %v", err)
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("recurring expense with ID %s not found to update", id)
	}

	var deleteQuery string
	if updateAll {
		deleteQuery = `DELETE FROM expenses WHERE recurring_id = ?`
		_, err = tx.Exec(deleteQuery, id)
	} else {
		deleteQuery = `DELETE FROM expenses WHERE recurring_id = ? AND date > ?`
		_, err = tx.Exec(deleteQuery, id, time.Now())
	}
	if err != nil {
		return fmt.Errorf("failed to delete old expense instances for update: %v", err)
	}

	expensesToAdd := generateExpensesFromRecurring(recurringExpense, !updateAll)
	if len(expensesToAdd) > 0 {
		stmt, err := tx.Prepare(`
			INSERT INTO expenses (id, recurring_id, name, category, amount, currency, date, tags)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %v", err)
		}
		defer stmt.Close()
		for _, exp := range expensesToAdd {
			expTagsJSON, _ := json.Marshal(exp.Tags)
			_, err = stmt.Exec(exp.ID, exp.RecurringID, exp.Name, exp.Category, exp.Amount, exp.Currency, exp.Date.Format(time.RFC3339), string(expTagsJSON))
			if err != nil {
				return fmt.Errorf("failed to insert expense: %v", err)
			}
		}
	}
	return tx.Commit()
}

func (s *sqliteStore) RemoveRecurringExpense(id string, removeAll bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()
	res, err := tx.Exec(`DELETE FROM recurring_expenses WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("failed to delete recurring expense rule: %v", err)
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("recurring expense with ID %s not found", id)
	}

	var deleteQuery string
	if removeAll {
		deleteQuery = `DELETE FROM expenses WHERE recurring_id = ?`
		_, err = tx.Exec(deleteQuery, id)
	} else {
		deleteQuery = `DELETE FROM expenses WHERE recurring_id = ? AND date > ?`
		_, err = tx.Exec(deleteQuery, id, time.Now())
	}
	if err != nil {
		return fmt.Errorf("failed to delete expense instances: %v", err)
	}
	return tx.Commit()
}
