package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"distributed-job-system/internal/config"
	"distributed-job-system/internal/models"
	"distributed-job-system/internal/repository"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

type JWTClaims struct {
	UserID         uuid.UUID `json:"user_id"`
	OrganizationID uuid.UUID `json:"organization_id"`
	Email          string    `json:"email"`
	Role           string    `json:"role"`
	jwt.RegisteredClaims
}

type AuthService struct {
	repo   *repository.AuthRepository
	config *config.Config
}

var ErrEmailAlreadyRegistered = errors.New("email is already registered")

func NewAuthService(repo *repository.AuthRepository, cfg *config.Config) *AuthService {
	return &AuthService{
		repo:   repo,
		config: cfg,
	}
}

// RegisterOrganizationAndUser handles registering a tenant and root administrator
func (s *AuthService) Register(ctx context.Context, orgName, email, password string) (*models.User, *models.Organization, string, string, error) {
	// Check if user already exists
	_, err := s.repo.GetUserByEmail(ctx, email)
	if err == nil {
		return nil, nil, "", "", errors.New("email is already registered")
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, nil, "", "", fmt.Errorf("failed to hash password: %w", err)
	}

	org, err := s.repo.CreateOrganization(ctx, orgName)
	if err != nil {
		return nil, nil, "", "", fmt.Errorf("failed to create organization: %w", err)
	}

	user, err := s.repo.CreateUser(ctx, org.ID, email, string(hashedPassword), "admin")
	if err != nil {
		return nil, nil, "", "", fmt.Errorf("failed to create user: %w", err)
	}

	accessToken, err := s.GenerateAccessToken(user)
	if err != nil {
		return nil, nil, "", "", err
	}

	refreshToken, err := s.generateAndStoreRefreshToken(ctx, user.ID)
	if err != nil {
		return nil, nil, "", "", err
	}

	return user, org, accessToken, refreshToken, nil
}

// Login verifies credentials and returns access and rotating refresh tokens
func (s *AuthService) Login(ctx context.Context, email, password string) (*models.User, string, string, error) {
	user, err := s.repo.GetUserByEmail(ctx, email)
	if err != nil {
		return nil, "", "", errors.New("invalid email or password")
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, "", "", errors.New("invalid email or password")
	}

	accessToken, err := s.GenerateAccessToken(user)
	if err != nil {
		return nil, "", "", err
	}

	refreshToken, err := s.generateAndStoreRefreshToken(ctx, user.ID)
	if err != nil {
		return nil, "", "", err
	}

	return user, accessToken, refreshToken, nil
}

func (s *AuthService) CreateMember(ctx context.Context, orgID uuid.UUID, email, password string) (*models.User, error) {
	_, err := s.repo.GetUserByEmail(ctx, email)
	if err == nil {
		return nil, ErrEmailAlreadyRegistered
	}
	if !errors.Is(err, repository.ErrNotFound) {
		return nil, fmt.Errorf("failed to check email: %w", err)
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("failed to hash password: %w", err)
	}

	user, err := s.repo.CreateUser(ctx, orgID, email, string(hashedPassword), "member")
	if err != nil {
		return nil, fmt.Errorf("failed to create member: %w", err)
	}
	return user, nil
}

func (s *AuthService) ListOrganizationUsers(ctx context.Context, orgID uuid.UUID) ([]*models.User, error) {
	return s.repo.ListUsersByOrganization(ctx, orgID)
}

// Refresh handles single-use refresh token rotation
func (s *AuthService) Refresh(ctx context.Context, rawRefreshToken string) (string, string, error) {
	userID, err := s.repo.ValidateAndRevokeRefreshToken(ctx, rawRefreshToken)
	if err != nil {
		return "", "", err
	}

	user, err := s.repo.GetUserByID(ctx, userID)
	if err != nil {
		return "", "", errors.New("user not found")
	}

	newAccessToken, err := s.GenerateAccessToken(user)
	if err != nil {
		return "", "", err
	}

	newRefreshToken, err := s.generateAndStoreRefreshToken(ctx, user.ID)
	if err != nil {
		return "", "", err
	}

	return newAccessToken, newRefreshToken, nil
}

// Logout revokes all active refresh tokens for the given user session
func (s *AuthService) Logout(ctx context.Context, userID uuid.UUID) error {
	return s.repo.RevokeAllUserTokens(ctx, userID)
}

// GenerateAccessToken creates a signed JWT access token for a user
func (s *AuthService) GenerateAccessToken(user *models.User) (string, error) {
	claims := JWTClaims{
		UserID:         user.ID,
		OrganizationID: user.OrganizationID,
		Email:          user.Email,
		Role:           user.Role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(s.config.JWTAccessExpiry)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "distributed-job-system",
			Subject:   user.ID.String(),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(s.config.JWTSecret))
}

func (s *AuthService) generateAndStoreRefreshToken(ctx context.Context, userID uuid.UUID) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	rawToken := hex.EncodeToString(b)
	expiresAt := time.Now().Add(s.config.JWTRefreshExpiry)

	err := s.repo.StoreRefreshToken(ctx, userID, rawToken, expiresAt)
	if err != nil {
		return "", err
	}

	return rawToken, nil
}

// ValidateAccessToken parses and validates JWT token string
func (s *AuthService) ValidateAccessToken(tokenStr string) (*JWTClaims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(s.config.JWTSecret), nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}), jwt.WithIssuer("distributed-job-system"))

	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*JWTClaims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token")
	}

	return claims, nil
}
