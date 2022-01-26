using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Raven.Server.Commercial.LetsEncrypt
{
    public static class LetsEncryptByTools
    {
        private const string AcmeClientUrl = "https://acme-v02.api.letsencrypt.org/directory";
        private static readonly string[] DnsBridgeActions = {"user-domains","domain-availability","claim"};

        public static async Task<byte[]> SetupLetsEncryptByRvn(SetupInfo setupInfo, string settingsPath, SetupProgressAndResult setupProgressAndResult, string dataFolder, CancellationToken token)
        {
            setupProgressAndResult.AddInfo("Setting up RavenDB in Let's Encrypt security mode.");

            if (ZipFileHelper.IsValidEmail(setupInfo.Email) == false)
                throw new ArgumentException("Invalid e-mail format" + setupInfo.Email);

            var acmeClient = new LetsEncryptClient(AcmeClientUrl);

            await acmeClient.Init(setupInfo.Email, token);
            setupProgressAndResult.AddInfo($"Getting challenge(s) from Let's Encrypt. Using e-mail: {setupInfo.Email}.");

            (string Challenge, LetsEncryptClient.CachedCertificateResult Cache) challengeResult;
            try
            {
                challengeResult = await InitialLetsEncryptChallenge(setupInfo, acmeClient, token);
                setupProgressAndResult.AddInfo(challengeResult.Challenge != null
                    ? "Successfully received challenge(s) information from Let's Encrypt."
                    : "Using cached Let's Encrypt certificate.");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to initialize lets encrypt challenge: " + e);

            }

            try
            {
                var registrationInfo = new RegistrationInfo
                {
                    License = setupInfo.License,
                    Domain = setupInfo.Domain,
                    Challenge = challengeResult.Challenge,
                    RootDomain = setupInfo.RootDomain,
                };
                var serializeObject = JsonConvert.SerializeObject(registrationInfo);

                foreach (var action in DnsBridgeActions)
                {
                    try
                    {
                        var content = new StringContent(serializeObject, Encoding.UTF8, "application/json");
                        var response = await ApiHttpClient.Instance.PostAsync($"/api/v1/dns-n-cert/{action}", content, CancellationToken.None).ConfigureAwait(false);
                        response.EnsureSuccessStatusCode();
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Failed to perform the given action: {action}", e);
                    }
                }            
           
                await RavenDnsRecordHelper.UpdateDnsRecordsTask(new RavenDnsRecordHelper.UpdateDnsRecordParameters
                {
                    Challenge = challengeResult.Challenge,
                    SetupInfo = setupInfo,
                    Progress = setupProgressAndResult,
                    Token = CancellationToken.None
                });
                setupProgressAndResult.AddInfo($"Updating DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}.");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to update DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}",
                    e);
            }

            setupProgressAndResult.AddInfo($"Successfully updated DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}");
            setupProgressAndResult.AddInfo("Completing Let's Encrypt challenge(s)...");

            await ZipFileHelper.CompleteAuthorizationAndGetCertificate(new ZipFileHelper.CompleteAuthorizationAndGetCertificateParameters
            {
                OnValidationSuccessful = () =>
                {
                    setupProgressAndResult.AddInfo("Let's Encrypt challenge(s) completed successfully.");
                    setupProgressAndResult.AddInfo("Acquiring certificate.");
                },
                SetupInfo = setupInfo,
                Client = acmeClient,
                ChallengeResult = challengeResult,
                Token = CancellationToken.None,
            });

            setupProgressAndResult.AddInfo("Successfully acquired certificate from Let's Encrypt.");
            setupProgressAndResult.AddInfo("Starting validation.");

            try
            {
                var zipFile = await ZipFileHelper.CompleteClusterConfigurationAndGetSettingsZip(new ZipFileHelper.CompleteClusterConfigurationParameters
                {
                    Progress = setupProgressAndResult,
                    OnProgress = null,
                    SetupInfo = setupInfo,
                    SetupMode = SetupMode.None,
                    SettingsPath = settingsPath,
                    OnGetCertificatePath = (getCertificatePath) => Task.Run(() =>Path.Combine(dataFolder,getCertificatePath), token),
                    LicenseType = LicenseType.None,
                    Token = CancellationToken.None,
                });
                
                return zipFile;
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to create the configuration settings.", e);
            }
        }

        public static async Task<byte[]> SetupOwnCertByRvn(SetupInfo setupInfo, string settingsPath, SetupProgressAndResult setupProgressAndResult, CancellationToken token)
        {
            var zipFile = Array.Empty<byte>();
            try
            {
                
                setupProgressAndResult.AddInfo("Setting up RavenDB in 'Secured Mode'.");
                setupProgressAndResult.AddInfo("Starting validation.");

                if (ZipFileHelper.IsValidEmail(setupInfo.Email) == false)
                    throw new ArgumentException("Invalid e-mail format" + setupInfo.Email);

                try
                {
                     zipFile = await ZipFileHelper.CompleteClusterConfigurationAndGetSettingsZip(new ZipFileHelper.CompleteClusterConfigurationParameters
                    {
                        Progress = setupProgressAndResult,
                        OnProgress = null,
                        SetupInfo = setupInfo,
                        SetupMode = SetupMode.None,
                        SettingsPath = settingsPath,
                        LicenseType = LicenseType.None,
                        Token = CancellationToken.None,
                        CertificateValidationKeyUsages = true
                    });

                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Failed to create the configuration settings.", e);
                }

                setupProgressAndResult.Processed++;
                setupProgressAndResult.AddInfo("Configuration settings created.");
                setupProgressAndResult.AddInfo("Setting up RavenDB in 'Secured Mode' finished successfully.");
                return zipFile;
            }
            catch (Exception e)
            {
                const string str = "Setting up RavenDB in 'Secured Mode' failed.";
                setupProgressAndResult.AddError(str, e);
                throw new InvalidOperationException(str, e);
            }
        }

        public static async Task<(string Challenge, LetsEncryptClient.CachedCertificateResult Cache)> InitialLetsEncryptChallenge(
            SetupInfo setupInfo,
            LetsEncryptClient client,
            CancellationToken token)
        {
            try
            {
                var host = (setupInfo.Domain + "." + setupInfo.RootDomain).ToLowerInvariant();
                var wildcardHost = "*." + host;
                if (client.TryGetCachedCertificate(wildcardHost, out var certBytes))
                    return (null, certBytes);

                var result = await client.NewOrder(new[] {wildcardHost}, token);

                result.TryGetValue(host, out var challenge);
                // we may already be authorized for this?
                return (challenge, null);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to receive challenge(s) information from Let's Encrypt.", e);
            }
        }
    }
}
