namespace WiserTaskScheduler.Core.Enums
{
    public enum OAuthState
    {
        SuccessfullyRequestedNewToken,
        UsingAlreadyExistingToken,
        AuthenticationFailed,
        RefreshTokenFailed,
        WaitingForManualAuthentication,
        NotEnoughInformation
    }
}