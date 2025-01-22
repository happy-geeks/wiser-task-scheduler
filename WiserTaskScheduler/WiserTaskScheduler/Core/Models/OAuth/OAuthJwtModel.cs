using System.Xml.Serialization;

namespace WiserTaskScheduler.Core.Models.OAuth;

/// <summary>
/// Settings model to build the JWT token with.
/// </summary>
[XmlType("Jwt")]
public class OAuthJwtModel
{
    /// <summary>
    /// Gets or sets the expiry time of the token in seconds. Needed for the exp claim.
    /// </summary>
    public int ExpirationTime { get; set; } = 600;

    /// <summary>
    /// Gets or sets the issuer of the token. Needed for the iss claim.
    /// </summary>
    public string Issuer { get; set; }

    /// <summary>
    /// Gets or sets the subject of the token. Needed for the sub claim.
    /// </summary>
    public string Subject { get; set; }

    /// <summary>
    /// Gets or sets the audience of the token. Needed for the aud claim.
    /// </summary>
    public string Audience { get; set; }

    /// <summary>
    /// Gets or sets the location of the certificate to use.
    /// </summary>
    public string CertificateLocation { get; set; }

    /// <summary>
    /// Gets or sets the password of the certificate.
    /// </summary>
    public string CertificatePassword { get; set; }

    /// <summary>
    /// Gets or sets additional claims to add to the token.
    /// </summary>
    [XmlArray("Claims")]
    [XmlArrayItem(typeof(ClaimModel))]
    public ClaimModel[] Claims { get; set; } = [];
}