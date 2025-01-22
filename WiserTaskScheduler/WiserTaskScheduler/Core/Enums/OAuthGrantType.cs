namespace WiserTaskScheduler.Core.Enums;

public enum OAuthGrantType
{
	NotSet,
	AuthCode,
	AuthCodeWithPKCE,
	Implicit,
	PasswordCredentials,
	ClientCredentials,
	RefreshToken
}