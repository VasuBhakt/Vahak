package store

import (
	"context"
	"fmt"
	"time"

	"github.com/VasuBhakt/vahak/internal/models"
	"github.com/google/uuid"
)

func (s *Store) CreateUser(ctx context.Context, user *models.User) error {
	query := `INSERT INTO users (
		id, email, username, first_name, last_name, password, verification_token, verification_token_expiry, verified, created_at, updated_at
	) VALUES (
	 	$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
	)`

	_, err := s.db.Exec(ctx, query,
		user.ID,
		user.Email,
		user.Username,
		user.FirstName,
		user.LastName,
		user.Password,
		user.VerificationToken,
		user.VerificationTokenExpiry,
		user.Verified,
		user.CreatedAt,
		user.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to create user: %w", err)
	}
	return nil
}

func (s *Store) GetUserByEmailOrUsername(ctx context.Context, identifier string) (*models.User, error) {
	query := `SELECT id, email, username, password FROM users WHERE email = $1 OR username = $1`
	var user models.User
	err := s.db.QueryRow(ctx, query, identifier).Scan(&user.ID, &user.Email, &user.Username, &user.Password)

	if err != nil {
		return nil, fmt.Errorf("user not found: %w", err)
	}

	return &user, nil
}

func (s *Store) GetUserById(ctx context.Context, id uuid.UUID) (*models.User, error) {
	query := `SELECT id, email, username, first_name, last_name FROM users WHERE id = $1`
	var user models.User
	err := s.db.QueryRow(ctx, query, id).Scan(&user.ID, &user.Email, &user.Username, &user.FirstName, &user.LastName)
	if err != nil {
		return nil, fmt.Errorf("user not found: %w", err)
	}
	return &user, nil
}

func (s *Store) GetEndpointsByUser(ctx context.Context, userID uuid.UUID) ([]models.Endpoint, error) {
	query := `SELECT id, name, target_url, transformer_script FROM endpoints WHERE user_id = $1`
	var endpoints []models.Endpoint
	rows, err := s.db.Query(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get endpoints: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var endpoint models.Endpoint
		err := rows.Scan(&endpoint.ID, &endpoint.Name, &endpoint.TargetURL, &endpoint.TransformerScript)
		if err != nil {
			return nil, fmt.Errorf("failed to scan endpoint: %w", err)
		}
		endpoints = append(endpoints, endpoint)
	}
	return endpoints, nil
}

func (s *Store) UpdateUserById(ctx context.Context, id uuid.UUID, user *models.User) (*models.User, error) {
	query := `UPDATE users SET email = $1, username = $2, first_name = $3, last_name = $4, updated_at = NOW() WHERE id = $5
			RETURNING id, email, username, first_name, last_name`

	var updatedUser models.User

	err := s.db.QueryRow(ctx, query,
		user.Email,
		user.Username,
		user.FirstName,
		user.LastName,
		id,
	).Scan(
		&updatedUser.ID,
		&updatedUser.Email,
		&updatedUser.Username,
		&updatedUser.FirstName,
		&updatedUser.LastName,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to update user: %w", err)
	}

	return &updatedUser, nil
}

func (s *Store) DeleteUserById(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM users WHERE id = $1`
	_, err := s.db.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}
	return nil
}

func (s *Store) GetUserByVerificationToken(ctx context.Context, token string) (uuid.UUID, error) {
	query := `
		SELECT id 
		FROM users 
		WHERE verification_token = $1 
		AND verification_token_expiry > NOW() 
		AND verified = FALSE
	`
	var id uuid.UUID
	err := s.db.QueryRow(ctx, query, token).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid or expired verification token: %w", err)
	}
	return id, nil
}

func (s *Store) UpdateVerificationStatus(ctx context.Context, id uuid.UUID, verified bool) error {
	query := `UPDATE users SET verified = $1, verification_token = null, verification_token_expiry = null, updated_at = NOW() WHERE id = $2`
	_, err := s.db.Exec(ctx, query, verified, id)
	if err != nil {
		return fmt.Errorf("failed to update verification: %w", err)
	}
	return nil
}

func (s *Store) SetForgotPasswordToken(ctx context.Context, id uuid.UUID, token string, expiry time.Time) error {
	query := `UPDATE users SET forgot_password_token = $1, forgot_password_token_expiry = $2, updated_at = NOW() WHERE id = $3`
	_, err := s.db.Exec(ctx, query, token, expiry, id)
	if err != nil {
		return fmt.Errorf("failed to set forgot password token: %w", err)
	}
	return nil
}

func (s *Store) GetUserByForgotPasswordToken(ctx context.Context, token string) (uuid.UUID, time.Time, error) {
	query := `SELECT id, forgot_password_token_expiry FROM users WHERE forgot_password_token = $1 AND forgot_password_token_expiry > NOW()`
	var id uuid.UUID
	var expiry time.Time
	err := s.db.QueryRow(ctx, query, token).Scan(&id, &expiry)
	if err != nil {
		return uuid.Nil, time.Time{}, fmt.Errorf("failed to get forgot password token: %w", err)
	}
	return id, expiry, nil
}

func (s *Store) UpdatePassword(ctx context.Context, id uuid.UUID, password string) error {
	query := `UPDATE users SET password = $1, forgot_password_token = null, forgot_password_token_expiry = null, updated_at = NOW() WHERE id = $2`
	_, err := s.db.Exec(ctx, query, password, id)
	if err != nil {
		return fmt.Errorf("failed to update password: %w", err)
	}
	return nil
}
