using Newtonsoft.Json;

namespace WiserTaskScheduler.Modules.Wiser.Models
{
    /// <summary>
    /// A model for the response of the Wiser API login request.
    /// </summary>
    public class WiserLoginResponseModel
    {
        /// <summary>
        /// Gets or sets the access token.
        /// </summary>
        [JsonProperty("access_token")]
        public string AccessToken { get; set; }

        /// <summary>
        /// Gets or sets the number of seconds the <see cref="AccessToken"/> expires in.
        /// </summary>
        [JsonProperty("expires_in")]
        public int ExpiresIn { get; set; }

        /// <summary>
        /// Gets or sets the refresh token.
        /// </summary>
        [JsonProperty("refresh_token")]
        public string RefreshToken { get; set; }
    }
}
