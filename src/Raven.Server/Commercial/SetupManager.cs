﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Org.BouncyCastle.Asn1;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto.Prng;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Https;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.Cli;
using Raven.Server.Web.Authentication;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Json.Sync;
using Sparrow.Server.Platform.Posix;
using Sparrow.Threading;
using Sparrow.Utils;
using OpenFlags = System.Security.Cryptography.X509Certificates.OpenFlags;
using StudioConfiguration = Raven.Client.Documents.Operations.Configuration.StudioConfiguration;

namespace Raven.Server.Commercial
{
    public static class SetupManager
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseManager>("Server");
        public const string GoogleDnsApi = "https://dns.google.com";

        public static string BuildHostName(string nodeTag, string userDomain, string rootDomain)
        {
            return $"{nodeTag}.{userDomain}.{rootDomain}".ToLower();
        }

        public static async Task<string> LetsEncryptAgreement(string email, ServerStore serverStore)
        {
            if (IsValidEmail(email) == false)
                throw new ArgumentException("Invalid e-mail format" + email);

            var acmeClient = new LetsEncryptClient(serverStore.Configuration.Core.AcmeUrl);
            await acmeClient.Init(email);
            return acmeClient.GetTermsOfServiceUri();
        }

        public static async Task<IOperationResult> SetupSecuredTask(Action<IOperationProgress> onProgress, SetupInfo setupInfo, ServerStore serverStore,
            CancellationToken token)
        {
            var progress = new SetupProgressAndResult {Processed = 0, Total = 2};

            try
            {
                AssertNoClusterDefined(serverStore);

                progress.AddInfo("Setting up RavenDB in 'Secured Mode'.");
                progress.AddInfo("Starting validation.");
                onProgress(progress);

                await ValidateSetupInfo(SetupMode.Secured, setupInfo, serverStore);

                try
                {
                    await ValidateServerCanRunWithSuppliedSettings(setupInfo, serverStore, SetupMode.Secured, token);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Validation failed.", e);
                }

                progress.Processed++;
                progress.AddInfo("Validation is successful.");
                progress.AddInfo("Creating new RavenDB configuration settings.");
                onProgress(progress);

                try
                {
                    progress.SettingsZipFile =
                        await CompleteClusterConfigurationAndGetSettingsZip(onProgress, progress, SetupMode.Secured, setupInfo, serverStore, token);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Could not create configuration settings.", e);
                }

                progress.Processed++;
                progress.AddInfo("Configuration settings created.");
                progress.AddInfo("Setting up RavenDB in 'Secured Mode' finished successfully.");
                onProgress(progress);
            }
            catch (Exception e)
            {
                LogErrorAndThrow(onProgress, progress, "Setting up RavenDB in 'Secured Mode' failed.", e);
            }

            return progress;
        }

        public static async Task<X509Certificate2> RefreshLetsEncryptTask(SetupInfo setupInfo, ServerStore serverStore, CancellationToken token)
        {
            if (Logger.IsOperationsEnabled)
                Logger.Operations($"Getting challenge(s) from Let's Encrypt. Using e-mail: {setupInfo.Email}.");

            var acmeClient = new LetsEncryptClient(serverStore.Configuration.Core.AcmeUrl);
            await acmeClient.Init(setupInfo.Email, token);

            // here we explicitly want to refresh the cert, so we don't want it cached
            var cacheKeys = setupInfo.NodeSetupInfos.Select(node => BuildHostName(node.Key, setupInfo.Domain, setupInfo.RootDomain)).ToList();
            acmeClient.ResetCachedCertificate(cacheKeys);

            var challengeResult = await InitialLetsEncryptChallenge(setupInfo, acmeClient, token);

            if (Logger.IsOperationsEnabled)
                Logger.Operations($"Updating DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}.");

            try
            {
                await UpdateDnsRecordsForCertificateRefreshTask(challengeResult.Challenge, setupInfo, token);

                // Cache the current DNS topology so we can check it again
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to update DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}",
                    e);
            }

            if (Logger.IsOperationsEnabled)
                Logger.Operations($"Successfully updated DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}");

            var cert = await CompleteAuthorizationAndGetCertificate(() =>
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations("Let's encrypt validation successful, acquiring certificate now...");
                },
                setupInfo,
                acmeClient,
                challengeResult,
                serverStore,
                token);

            if (Logger.IsOperationsEnabled)
                Logger.Operations("Successfully acquired certificate from Let's Encrypt.");

            return cert;
        }

        public static async Task<IOperationResult> ContinueClusterSetupTask(Action<IOperationProgress> onProgress, ContinueSetupInfo continueSetupInfo,
            ServerStore serverStore, CancellationToken token)
        {
            var progress = new SetupProgressAndResult {Processed = 0, Total = 4};

            try
            {
                AssertNoClusterDefined(serverStore);

                progress.AddInfo($"Continuing cluster setup on node {continueSetupInfo.NodeTag}.");
                onProgress(progress);

                byte[] zipBytes;

                try
                {
                    zipBytes = Convert.FromBase64String(continueSetupInfo.Zip);
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Unable to parse the {nameof(continueSetupInfo.Zip)} property, expected a Base64 value", e);
                }

                progress.Processed++;
                progress.AddInfo("Extracting setup settings and certificates from zip file.");
                onProgress(progress);

                using (serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    byte[] serverCertBytes;
                    BlittableJsonReaderObject settingsJsonObject;
                    Dictionary<string, string> otherNodesUrls;
                    string firstNodeTag;
                    License license;
                    X509Certificate2 clientCert;
                    X509Certificate2 serverCert;
                    try
                    {
                        settingsJsonObject = ExtractCertificatesAndSettingsJsonFromZip(zipBytes, continueSetupInfo.NodeTag, context, out serverCertBytes,
                            out serverCert, out clientCert, out firstNodeTag, out otherNodesUrls, out license);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Unable to extract setup information from the zip file.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Starting validation.");
                    onProgress(progress);

                    try
                    {
                        await ValidateServerCanRunOnThisNode(settingsJsonObject, serverCert, serverStore, continueSetupInfo.NodeTag, token);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Validation failed.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Validation is successful.");
                    progress.AddInfo("Writing configuration settings and certificate.");
                    onProgress(progress);

                    try
                    {
                        await CompleteConfigurationForNewNode(onProgress, progress, continueSetupInfo, settingsJsonObject, serverCertBytes, serverCert,
                            clientCert, serverStore, firstNodeTag, otherNodesUrls, license, context);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Could not complete configuration for new node.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Configuration settings created.");
                    progress.AddInfo("Setting up RavenDB in 'Secured Mode' finished successfully.");
                    onProgress(progress);

                    settingsJsonObject.Dispose();
                }
            }
            catch (Exception e)
            {
                LogErrorAndThrow(onProgress, progress, $"Cluster setup on node {continueSetupInfo.NodeTag} has failed", e);
            }

            return progress;
        }

        public static BlittableJsonReaderObject ExtractCertificatesAndSettingsJsonFromZip(byte[] zipBytes, string currentNodeTag, JsonOperationContext context,
            out byte[] certBytes, out X509Certificate2 serverCert, out X509Certificate2 clientCert, out string firstNodeTag,
            out Dictionary<string, string> otherNodesUrls, out License license)
        {
            certBytes = null;
            byte[] clientCertBytes = null;
            BlittableJsonReaderObject currentNodeSettingsJson = null;
            license = null;

            otherNodesUrls = new Dictionary<string, string>();

            firstNodeTag = "A";

            using (var msZip = new MemoryStream(zipBytes))
            using (var archive = new ZipArchive(msZip, ZipArchiveMode.Read, false))
            {
                foreach (var entry in archive.Entries)
                {
                    // try to find setup.json file first, as we make decisions based on its contents
                    if (entry.Name.Equals("setup.json"))
                    {
                        var json = context.Sync.ReadForMemory(entry.Open(), "license/json");

                        SetupSettings setupSettings = JsonDeserializationServer.SetupSettings(json);
                        firstNodeTag = setupSettings.Nodes[0].Tag;

                        // Since we allow to customize node tags, we stored information about the order of nodes into setup.json file
                        // The first node is the one in which the cluster should be initialized.
                        // If the file isn't found, it means we are using a zip which was created in the old codebase => first node has the tag 'A'
                    }
                }

                foreach (var entry in archive.Entries)
                {
                    if (entry.FullName.StartsWith($"{currentNodeTag}/") && entry.Name.EndsWith(".pfx"))
                    {
                        using (var ms = new MemoryStream())
                        {
                            entry.Open().CopyTo(ms);
                            certBytes = ms.ToArray();
                        }
                    }

                    if (entry.Name.StartsWith("admin.client.certificate") && entry.Name.EndsWith(".pfx"))
                    {
                        using (var ms = new MemoryStream())
                        {
                            entry.Open().CopyTo(ms);
                            clientCertBytes = ms.ToArray();
                        }
                    }

                    if (entry.Name.Equals("license.json"))
                    {
                        var json = context.Sync.ReadForMemory(entry.Open(), "license/json");
                        license = JsonDeserializationServer.License(json);
                    }

                    if (entry.Name.Equals("settings.json"))
                    {
                        using (var settingsJson = context.Sync.ReadForMemory(entry.Open(), "settings-json-from-zip"))
                        {
                            settingsJson.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string publicServerUrl);

                            if (entry.FullName.StartsWith($"{currentNodeTag}/"))
                            {
                                currentNodeSettingsJson = settingsJson.Clone(context);
                            }

                            // This is for the case where we take the zip file and use it to setup the first node as well.
                            // If this is the first node, we must collect the urls of the other nodes so that
                            // we will be able to add them to the cluster when we bootstrap the cluster.
                            if (entry.FullName.StartsWith(firstNodeTag + "/") == false && publicServerUrl != null)
                            {
                                var tag = entry.FullName.Substring(0, entry.FullName.Length - "/settings.json".Length);
                                otherNodesUrls.Add(tag, publicServerUrl);
                            }
                        }
                    }
                }
            }

            if (certBytes == null)
                throw new InvalidOperationException($"Could not extract the server certificate of node '{currentNodeTag}'. Are you using the correct zip file?");
            if (clientCertBytes == null)
                throw new InvalidOperationException("Could not extract the client certificate. Are you using the correct zip file?");
            if (currentNodeSettingsJson == null)
                throw new InvalidOperationException($"Could not extract settings.json of node '{currentNodeTag}'. Are you using the correct zip file?");

            try
            {
                currentNodeSettingsJson.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePassword), out string certPassword);

                serverCert = new X509Certificate2(certBytes, certPassword,
                    X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.MachineKeySet);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to load the server certificate of node '{currentNodeTag}'.", e);
            }

            try
            {
                clientCert = new X509Certificate2(clientCertBytes, (string)null,
                    X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.MachineKeySet);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Unable to load the client certificate.", e);
            }

            return currentNodeSettingsJson;
        }

        public static async Task<LicenseStatus> GetUpdatedLicenseStatus(ServerStore serverStore, License currentLicense, Reference<License> updatedLicense = null)
        {
            var license = await serverStore.LicenseManager.GetUpdatedLicense(currentLicense).ConfigureAwait(false) ?? currentLicense;

            var licenseStatus = LicenseManager.GetLicenseStatus(license);
            if (licenseStatus.Expired)
                throw new LicenseExpiredException($"The provided license for {license.Name} has expired ({licenseStatus.Expiration})");

            if (updatedLicense != null)
                updatedLicense.Value = license;

            return licenseStatus;
        }

        public static async Task<IOperationResult> SetupLetsEncryptTask(Action<IOperationProgress> onProgress, SetupInfo setupInfo, ServerStore serverStore,
            CancellationToken token)
        {
            var progress = new SetupProgressAndResult {Processed = 0, Total = 4};

            try
            {
                var updatedLicense = new Reference<License>();
                await GetUpdatedLicenseStatus(serverStore, setupInfo.License, updatedLicense).ConfigureAwait(false);
                setupInfo.License = updatedLicense.Value;

                AssertNoClusterDefined(serverStore);
                progress.AddInfo("Setting up RavenDB in Let's Encrypt security mode.");
                onProgress(progress);
                try
                {
                    await ValidateSetupInfo(SetupMode.LetsEncrypt, setupInfo, serverStore);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Validation of supplied settings failed.", e);
                }

                progress.AddInfo($"Getting challenge(s) from Let's Encrypt. Using e-mail: {setupInfo.Email}.");
                onProgress(progress);

                var acmeClient = new LetsEncryptClient(serverStore.Configuration.Core.AcmeUrl);
                await acmeClient.Init(setupInfo.Email, token);

                var challengeResult = await InitialLetsEncryptChallenge(setupInfo, acmeClient, token);

                progress.Processed++;
                progress.AddInfo(challengeResult.Challenge != null
                    ? "Successfully received challenge(s) information from Let's Encrypt."
                    : "Using cached Let's Encrypt certificate.");

                progress.AddInfo($"Updating DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}.");

                onProgress(progress);

                try
                {
                    await UpdateDnsRecordsTask(onProgress, progress, challengeResult.Challenge, setupInfo, token);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException(
                        $"Failed to update DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}", e);
                }

                progress.Processed++;
                progress.AddInfo($"Successfully updated DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}");
                progress.AddInfo("Completing Let's Encrypt challenge(s)...");
                onProgress(progress);

                await CompleteAuthorizationAndGetCertificate(() =>
                    {
                        progress.AddInfo("Let's Encrypt challenge(s) completed successfully.");
                        progress.AddInfo("Acquiring certificate.");
                        onProgress(progress);
                    },
                    setupInfo,
                    acmeClient,
                    challengeResult,
                    serverStore,
                    token);

                progress.Processed++;
                progress.AddInfo("Successfully acquired certificate from Let's Encrypt.");
                progress.AddInfo("Starting validation.");
                onProgress(progress);

                try
                {
                    await ValidateServerCanRunWithSuppliedSettings(setupInfo, serverStore, SetupMode.LetsEncrypt, token);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Validation failed.", e);
                }

                progress.Processed++;
                progress.AddInfo("Validation is successful.");
                progress.AddInfo("Creating new RavenDB configuration settings.");

                onProgress(progress);

                try
                {
                    progress.SettingsZipFile =
                        await CompleteClusterConfigurationAndGetSettingsZip(onProgress, progress, SetupMode.LetsEncrypt, setupInfo, serverStore, token);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Failed to create the configuration settings.", e);
                }

                progress.Processed++;
                progress.AddInfo("Configuration settings created.");
                progress.AddInfo("Setting up RavenDB in Let's Encrypt security mode finished successfully.");
                onProgress(progress);
            }
            catch (Exception e)
            {
                LogErrorAndThrow(onProgress, progress, "Setting up RavenDB in Let's Encrypt security mode failed.", e);
            }

            return progress;
        }

        private static void AssertNoClusterDefined(ServerStore serverStore)
        {
            var allNodes = serverStore.GetClusterTopology().AllNodes;
            if (allNodes.Count > 1)
            {
                throw new InvalidOperationException("This node is part of an already configured cluster and cannot be setup automatically any longer." +
                                                    Environment.NewLine +
                                                    "Either setup manually by editing the 'settings.json' file or delete the existing cluster, restart the server and try running setup again." +
                                                    Environment.NewLine +
                                                    "Existing cluster nodes " + JsonConvert.SerializeObject(allNodes, Formatting.Indented)
                );
            }
        }

        private static async Task DeleteAllExistingCertificates(ServerStore serverStore)
        {
            // If a user repeats the setup process, there might be certificate leftovers in the cluster

            List<string> existingCertificateKeys;
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                existingCertificateKeys = serverStore.Cluster.GetCertificateThumbprintsFromCluster(context).ToList();
            }

            if (existingCertificateKeys.Count == 0)
                return;

            var res = await serverStore.SendToLeaderAsync(new DeleteCertificateCollectionFromClusterCommand(RaftIdGenerator.NewId()) {Names = existingCertificateKeys});

            await serverStore.Cluster.WaitForIndexNotification(res.Index);
        }

        private static async Task<X509Certificate2> CompleteAuthorizationAndGetCertificate(Action onValidationSuccessful, SetupInfo setupInfo, LetsEncryptClient client,
            (string Challange, LetsEncryptClient.CachedCertificateResult Cache) challengeResult, ServerStore serverStore, CancellationToken token)
        {
            if (challengeResult.Challange == null && challengeResult.Cache != null)
            {
                return BuildNewPfx(setupInfo, challengeResult.Cache.Certificate, challengeResult.Cache.PrivateKey);
            }

            try
            {
                await client.CompleteChallenges(token);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to Complete Let's Encrypt challenge(s).", e);
            }

            onValidationSuccessful();

            (X509Certificate2 Cert, RSA PrivateKey) result;
            try
            {
                var existingPrivateKey = serverStore.Server.Certificate?.Certificate?.GetRSAPrivateKey();
                result = await client.GetCertificate(existingPrivateKey, token);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to acquire certificate from Let's Encrypt.", e);
            }

            try
            {
                return BuildNewPfx(setupInfo, result.Cert, result.PrivateKey);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to build certificate from Let's Encrypt.", e);
            }
        }

        private static X509Certificate2 BuildNewPfx(SetupInfo setupInfo, X509Certificate2 certificate, RSA privateKey)
        {
            var certWithKey = certificate.CopyWithPrivateKey(privateKey);

            Pkcs12Store store = new Pkcs12StoreBuilder().Build();

            var chain = new X509Chain();
            chain.ChainPolicy.DisableCertificateDownloads = true;

            chain.Build(certificate);

            foreach (var item in chain.ChainElements)
            {
                var x509Certificate = DotNetUtilities.FromX509Certificate(item.Certificate);

                if (item.Certificate.Thumbprint == certificate.Thumbprint)
                {
                    var key = new AsymmetricKeyEntry(DotNetUtilities.GetKeyPair(certWithKey.GetRSAPrivateKey()).Private);
                    store.SetKeyEntry(x509Certificate.SubjectDN.ToString(), key, new[] {new X509CertificateEntry(x509Certificate)});
                    continue;
                }

                store.SetCertificateEntry(item.Certificate.Subject, new X509CertificateEntry(x509Certificate));
            }

            var memoryStream = new MemoryStream();
            store.Save(memoryStream, Array.Empty<char>(), new SecureRandom(new CryptoApiRandomGenerator()));
            var certBytes = memoryStream.ToArray();

            Debug.Assert(certBytes != null);
            setupInfo.Certificate = Convert.ToBase64String(certBytes);

            return new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
        }

        private static async Task<(string Challenge, LetsEncryptClient.CachedCertificateResult Cache)> InitialLetsEncryptChallenge(
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

        private static void LogErrorAndThrow(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, string msg, Exception e)
        {
            progress.AddError(msg, e);
            onProgress.Invoke(progress);
            throw new InvalidOperationException(msg, e);
        }

        private static async Task UpdateDnsRecordsForCertificateRefreshTask(
            string challenge,
            SetupInfo setupInfo, CancellationToken token)
        {
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, new CancellationTokenSource(TimeSpan.FromMinutes(15)).Token))
            {
                var registrationInfo = new RegistrationInfo
                {
                    License = setupInfo.License,
                    Domain = setupInfo.Domain,
                    Challenge = challenge,
                    RootDomain = setupInfo.RootDomain,
                    SubDomains = new List<RegistrationNodeInfo>()
                };

                foreach (var node in setupInfo.NodeSetupInfos)
                {
                    var regNodeInfo = new RegistrationNodeInfo {SubDomain = (node.Key + "." + setupInfo.Domain).ToLower(),};

                    registrationInfo.SubDomains.Add(regNodeInfo);
                }

                var serializeObject = JsonConvert.SerializeObject(registrationInfo);

                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Start update process for certificate. License Id: {registrationInfo.License.Id}, " +
                                      $"License Name: {registrationInfo.License.Name}, " +
                                      $"Domain: {registrationInfo.Domain}, " +
                                      $"RootDomain: {registrationInfo.RootDomain}");

                HttpResponseMessage response;
                try
                {
                    response = await ApiHttpClient.Instance.PostAsync("api/v1/dns-n-cert/register",
                        new StringContent(serializeObject, Encoding.UTF8, "application/json"), token).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Registration request to api.ravendb.net failed for: " + serializeObject, e);
                }

                var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                if (response.IsSuccessStatusCode == false)
                {
                    throw new InvalidOperationException(
                        $"Got unsuccessful response from registration request: {response.StatusCode}.{Environment.NewLine}{responseString}");
                }

                var id = JsonConvert.DeserializeObject<Dictionary<string, string>>(responseString).First().Value;

                try
                {
                    RegistrationResult registrationResult;
                    do
                    {
                        try
                        {
                            await Task.Delay(1000, cts.Token);
                            response = await ApiHttpClient.Instance.PostAsync("api/v1/dns-n-cert/registration-result?id=" + id,
                                    new StringContent(serializeObject, Encoding.UTF8, "application/json"), cts.Token)
                                .ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Registration-result request to api.ravendb.net failed.", e); //add the object we tried to send to error
                        }

                        responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                        if (response.IsSuccessStatusCode == false)
                        {
                            throw new InvalidOperationException(
                                $"Got unsuccessful response from registration-result request: {response.StatusCode}.{Environment.NewLine}{responseString}");
                        }

                        registrationResult = JsonConvert.DeserializeObject<RegistrationResult>(responseString);
                    } while (registrationResult.Status == "PENDING");
                }
                catch (Exception e)
                {
                    if (cts.IsCancellationRequested == false)
                        throw;
                    throw new TimeoutException("Request failed due to a timeout error", e);
                }
            }
        }

        private static async Task UpdateDnsRecordsTask(
            Action<IOperationProgress> onProgress,
            SetupProgressAndResult progress,
            string challenge,
            SetupInfo setupInfo,
            CancellationToken token)
        {
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, new CancellationTokenSource(TimeSpan.FromMinutes(15)).Token))
            {
                var registrationInfo = new RegistrationInfo
                {
                    License = setupInfo.License,
                    Domain = setupInfo.Domain,
                    Challenge = challenge,
                    RootDomain = setupInfo.RootDomain,
                    SubDomains = new List<RegistrationNodeInfo>()
                };

                foreach (var node in setupInfo.NodeSetupInfos)
                {
                    var regNodeInfo = new RegistrationNodeInfo
                    {
                        SubDomain = (node.Key + "." + setupInfo.Domain).ToLower(),
                        Ips = node.Value.ExternalIpAddress == null
                            ? node.Value.Addresses
                            : new List<string> {node.Value.ExternalIpAddress}
                    };

                    registrationInfo.SubDomains.Add(regNodeInfo);
                }

                progress.AddInfo($"Creating DNS record/challenge for node(s): {string.Join(", ", setupInfo.NodeSetupInfos.Keys)}.");

                onProgress(progress);

                if (registrationInfo.SubDomains.Count == 0 && registrationInfo.Challenge == null)
                {
                    // no need to update anything, can skip doing DNS update
                    progress.AddInfo("Cached DNS values matched, skipping DNS update");
                    return;
                }

                var serializeObject = JsonConvert.SerializeObject(registrationInfo);
                HttpResponseMessage response;
                try
                {
                    progress.AddInfo("Registering DNS record(s)/challenge(s) in api.ravendb.net.");
                    progress.AddInfo("Please wait between 30 seconds and a few minutes.");
                    onProgress(progress);
                    response = await ApiHttpClient.Instance.PostAsync("api/v1/dns-n-cert/register",
                        new StringContent(serializeObject, Encoding.UTF8, "application/json"), token).ConfigureAwait(false);
                    progress.AddInfo("Waiting for DNS records to update...");
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Registration request to api.ravendb.net failed for: " + serializeObject, e);
                }

                var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                if (response.IsSuccessStatusCode == false)
                {
                    throw new InvalidOperationException(
                        $"Got unsuccessful response from registration request: {response.StatusCode}.{Environment.NewLine}{responseString}");
                }

                if (challenge == null)
                {
                    var existingSubDomain =
                        registrationInfo.SubDomains.FirstOrDefault(x => x.SubDomain.StartsWith(setupInfo.LocalNodeTag + ".", StringComparison.OrdinalIgnoreCase));
                    if (existingSubDomain != null && new HashSet<string>(existingSubDomain.Ips).SetEquals(setupInfo.NodeSetupInfos[setupInfo.LocalNodeTag].Addresses))
                    {
                        progress.AddInfo("DNS update started successfully, since current node (" + setupInfo.LocalNodeTag +
                                         ") DNS record didn't change, not waiting for full DNS propagation.");
                        return;
                    }
                }

                var id = JsonConvert.DeserializeObject<Dictionary<string, string>>(responseString).First().Value;

                try
                {
                    RegistrationResult registrationResult;
                    var i = 1;
                    do
                    {
                        try
                        {
                            await Task.Delay(1000, cts.Token);
                            response = await ApiHttpClient.Instance.PostAsync("api/v1/dns-n-cert/registration-result?id=" + id,
                                    new StringContent(serializeObject, Encoding.UTF8, "application/json"), cts.Token)
                                .ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Registration-result request to api.ravendb.net failed.", e); //add the object we tried to send to error
                        }

                        responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                        if (response.IsSuccessStatusCode == false)
                        {
                            throw new InvalidOperationException(
                                $"Got unsuccessful response from registration-result request: {response.StatusCode}.{Environment.NewLine}{responseString}");
                        }

                        registrationResult = JsonConvert.DeserializeObject<RegistrationResult>(responseString);

                        if (i % 120 == 0)
                            progress.AddInfo("This is taking too long, you might want to abort and restart if this goes on like this...");
                        else if (i % 45 == 0)
                            progress.AddInfo("If everything goes all right, we should be nearly there...");
                        else if (i % 30 == 0)
                            progress.AddInfo("The DNS update is still pending, carry on just a little bit longer...");
                        else if (i % 15 == 0)
                            progress.AddInfo("Please be patient, updating DNS records takes time...");
                        else if (i % 5 == 0)
                            progress.AddInfo("Waiting...");

                        onProgress(progress);

                        i++;
                    } while (registrationResult.Status == "PENDING");

                    progress.AddInfo("Got successful response from api.ravendb.net.");
                    onProgress(progress);
                }
                catch (Exception e)
                {
                    if (cts.IsCancellationRequested == false)
                        throw;
                    throw new TimeoutException("Request failed due to a timeout error", e);
                }
            }
        }

        public static async Task AssertLocalNodeCanListenToEndpoints(SetupInfo setupInfo, ServerStore serverStore)
        {
            var localNode = setupInfo.NodeSetupInfos[setupInfo.LocalNodeTag];
            var localIps = new List<IPEndPoint>();

            // Because we can get from user either an ip or a hostname, we resolve the hostname and get the actual ips it is mapped to
            foreach (var hostnameOrIp in localNode.Addresses)
            {
                if (hostnameOrIp.Equals("0.0.0.0"))
                {
                    localIps.Add(new IPEndPoint(IPAddress.Parse(hostnameOrIp), localNode.Port));
                    localIps.Add(new IPEndPoint(IPAddress.Parse(hostnameOrIp), localNode.TcpPort));
                    continue;
                }

                foreach (var ip in await Dns.GetHostAddressesAsync(hostnameOrIp))
                {
                    localIps.Add(new IPEndPoint(IPAddress.Parse(ip.ToString()), localNode.Port));
                    localIps.Add(new IPEndPoint(IPAddress.Parse(ip.ToString()), localNode.TcpPort));
                }
            }

            var requestedEndpoints = localIps.ToArray();
            var currentServerEndpoints = serverStore.Server.ListenEndpoints.Addresses.Select(ip => new IPEndPoint(ip, serverStore.Server.ListenEndpoints.Port)).ToList();

            var ipProperties = IPGlobalProperties.GetIPGlobalProperties();
            IPEndPoint[] activeTcpListeners;
            try
            {
                activeTcpListeners = ipProperties.GetActiveTcpListeners();
            }
            catch (Exception)
            {
                // If GetActiveTcpListeners is not supported, skip the validation
                // See https://github.com/dotnet/corefx/issues/30909
                return;
            }

            foreach (var requestedEndpoint in requestedEndpoints)
            {
                if (activeTcpListeners.Contains(requestedEndpoint))
                {
                    if (currentServerEndpoints.Contains(requestedEndpoint))
                        continue; // OK... used by the current server

                    throw new InvalidOperationException(
                        $"The requested endpoint '{requestedEndpoint.Address}:{requestedEndpoint.Port}' is already in use by another process. You may go back in the wizard, change the settings and try again.");
                }
            }
        }

        public static async Task ValidateServerCanRunWithSuppliedSettings(SetupInfo setupInfo, ServerStore serverStore, SetupMode setupMode, CancellationToken token)
        {
            var localNode = setupInfo.NodeSetupInfos[setupInfo.LocalNodeTag];
            var localIps = new List<IPEndPoint>();

            foreach (var hostnameOrIp in localNode.Addresses)
            {
                if (hostnameOrIp.Equals("0.0.0.0"))
                {
                    localIps.Add(new IPEndPoint(IPAddress.Parse(hostnameOrIp), localNode.Port));
                    continue;
                }

                foreach (var ip in await Dns.GetHostAddressesAsync(hostnameOrIp))
                {
                    localIps.Add(new IPEndPoint(IPAddress.Parse(ip.ToString()), localNode.Port));
                }
            }

            var serverCert = setupInfo.GetX509Certificate();

            var localServerUrl =
                LetsEncryptUtils.GetServerUrlFromCertificate(serverCert, setupInfo, setupInfo.LocalNodeTag, localNode.Port, localNode.TcpPort, out _, out _);

            try
            {
                if (serverStore.Server.ListenEndpoints.Port == localNode.Port)
                {
                    var currentIps = serverStore.Server.ListenEndpoints.Addresses.ToList();

                    if (localIps.Count == 0 && currentIps.Count == 1 &&
                        (Equals(currentIps[0], IPAddress.Any) || Equals(currentIps[0], IPAddress.IPv6Any)))
                        return; // listen to any ip in this

                    if (localIps.All(ip => currentIps.Contains(ip.Address)))
                        return; // we already listen to all these IPs, no need to check
                }

                if (setupMode == SetupMode.LetsEncrypt)
                {
                    // In case an external ip was specified, this is the ip we update in the dns records. (not the one we bind to)
                    var ips = localNode.ExternalIpAddress == null
                        ? localIps.ToArray()
                        : new[] {new IPEndPoint(IPAddress.Parse(localNode.ExternalIpAddress), localNode.ExternalPort)};

                    await AssertDnsUpdatedSuccessfully(localServerUrl, ips, token);
                }

                // Here we send the actual ips we will bind to in the local machine.
                await SimulateRunningServer(serverStore, serverCert, localServerUrl, setupInfo.LocalNodeTag, localIps.ToArray(), localNode.Port,
                    serverStore.Configuration.ConfigPath, setupMode, token);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to simulate running the server with the supplied settings using: " + localServerUrl, e);
            }
        }

        public static async Task ValidateServerCanRunOnThisNode(BlittableJsonReaderObject settingsJsonObject, X509Certificate2 cert, ServerStore serverStore,
            string nodeTag, CancellationToken token)
        {
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string publicServerUrl);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string serverUrl);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ExternalIp), out string externalIp);

            var serverUrls = serverUrl.Split(";");
            var port = new Uri(serverUrls[0]).Port;
            var hostnamesOrIps = serverUrls.Select(url => new Uri(url).DnsSafeHost).ToArray();

            var localIps = new List<IPEndPoint>();

            foreach (var hostnameOrIp in hostnamesOrIps)
            {
                if (hostnameOrIp.Equals("0.0.0.0"))
                {
                    localIps.Add(new IPEndPoint(IPAddress.Parse(hostnameOrIp), port));
                    continue;
                }

                foreach (var ip in await Dns.GetHostAddressesAsync(hostnameOrIp))
                {
                    localIps.Add(new IPEndPoint(IPAddress.Parse(ip.ToString()), port));
                }
            }

            try
            {
                if (serverStore.Server.ListenEndpoints.Port == port)
                {
                    var currentIps = serverStore.Server.ListenEndpoints.Addresses.ToList();

                    if (localIps.Count == 0 && currentIps.Count == 1 &&
                        (Equals(currentIps[0], IPAddress.Any) || Equals(currentIps[0], IPAddress.IPv6Any)))
                        return; // listen to any ip in this

                    if (localIps.All(ip => currentIps.Contains(ip.Address)))
                        return; // we already listen to all these IPs, no need to check
                }

                if (setupMode == SetupMode.LetsEncrypt)
                {
                    var ips = string.IsNullOrEmpty(externalIp)
                        ? localIps.ToArray()
                        : new[] {new IPEndPoint(IPAddress.Parse(externalIp), port)};

                    await AssertDnsUpdatedSuccessfully(publicServerUrl, ips, token);
                }

                // Here we send the actual ips we will bind to in the local machine.
                await SimulateRunningServer(serverStore, cert, publicServerUrl, nodeTag, localIps.ToArray(), port, serverStore.Configuration.ConfigPath, setupMode,
                    token);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to simulate running the server with the supplied settings using: " + publicServerUrl, e);
            }
        }

        public static async Task ValidateSetupInfo(SetupMode setupMode, SetupInfo setupInfo, ServerStore serverStore)
        {
            if ((await SetupParameters.Get(serverStore)).IsDocker)
            {
                if (setupInfo.NodeSetupInfos[setupInfo.LocalNodeTag].Addresses.Any(ip => ip.StartsWith("127.")))
                {
                    throw new InvalidOperationException("When the server is running in Docker, you cannot bind to ip 127.X.X.X, please use the hostname instead.");
                }
            }

            if (setupMode == SetupMode.LetsEncrypt)
            {
                if (setupInfo.NodeSetupInfos.ContainsKey(setupInfo.LocalNodeTag) == false)
                    throw new ArgumentException($"At least one of the nodes must have the node tag '{setupInfo.LocalNodeTag}'.");
                if (IsValidEmail(setupInfo.Email) == false)
                    throw new ArgumentException("Invalid email address.");
                if (IsValidDomain(setupInfo.Domain + "." + setupInfo.RootDomain) == false)
                    throw new ArgumentException("Invalid domain name.");
                if (setupInfo.ClientCertNotAfter.HasValue && setupInfo.ClientCertNotAfter <= DateTime.UtcNow.Date)
                    throw new ArgumentException("The client certificate expiration date must be in the future.");
            }

            if (setupMode == SetupMode.Secured && string.IsNullOrWhiteSpace(setupInfo.Certificate))
                throw new ArgumentException($"{nameof(setupInfo.Certificate)} is a mandatory property for a secured setup");

            foreach (var node in setupInfo.NodeSetupInfos)
            {
                RachisConsensus.ValidateNodeTag(node.Key);

                if (node.Value.Port == 0)
                    setupInfo.NodeSetupInfos[node.Key].Port = 443;

                if (node.Value.TcpPort == 0)
                    setupInfo.NodeSetupInfos[node.Key].TcpPort = 38888;

                if (setupMode == SetupMode.LetsEncrypt &&
                    setupInfo.NodeSetupInfos[node.Key].Addresses.Any(ip => ip.Equals("0.0.0.0")) &&
                    string.IsNullOrWhiteSpace(setupInfo.NodeSetupInfos[node.Key].ExternalIpAddress))
                {
                    throw new ArgumentException("When choosing 0.0.0.0 as the ip address, you must provide an external ip to update in the DNS records.");
                }
            }

            await AssertLocalNodeCanListenToEndpoints(setupInfo, serverStore);
        }

        public static bool IsValidEmail(string email)
        {
            if (string.IsNullOrWhiteSpace(email))
                return false;
            try
            {
                var address = new System.Net.Mail.MailAddress(email);
                return address.Address == email;
            }
            catch
            {
                return false;
            }
        }

        private static bool IsValidDomain(string domain)
        {
            if (string.IsNullOrWhiteSpace(domain))
                return false;

            return Uri.CheckHostName(domain) != UriHostNameType.Unknown;
        }

 

        private static async Task CompleteConfigurationForNewNode(
            Action<IOperationProgress> onProgress,
            SetupProgressAndResult progress,
            ContinueSetupInfo continueSetupInfo,
            BlittableJsonReaderObject settingsJsonObject,
            byte[] serverCertBytes,
            X509Certificate2 serverCert,
            X509Certificate2 clientCert,
            ServerStore serverStore,
            string firstNodeTag,
            Dictionary<string, string> otherNodesUrls,
            License license,
            JsonOperationContext context)
        {
            try
            {
                serverStore.Engine.SetNewState(RachisState.Passive, null, serverStore.Engine.CurrentTerm, "During setup wizard, " +
                                                                                                          "making sure there is no cluster from previous installation.");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to delete previous cluster topology during setup.", e);
            }

            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePassword), out string certPassword);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string publicServerUrl);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePath), out string certificateFileName);

            serverStore.Server.Certificate =
                SecretProtection.ValidateCertificateAndCreateCertificateHolder("Setup", serverCert, serverCertBytes, certPassword, serverStore.LicenseManager.LicenseStatus.Type, true);

            if (continueSetupInfo.NodeTag.Equals(firstNodeTag))
            {
                await serverStore.EnsureNotPassiveAsync(publicServerUrl, firstNodeTag);

                await DeleteAllExistingCertificates(serverStore);

                if (setupMode == SetupMode.LetsEncrypt && license != null)
                {
                    await serverStore.EnsureNotPassiveAsync(skipLicenseActivation: true);
                    await serverStore.LicenseManager.ActivateAsync(license, RaftIdGenerator.DontCareId);
                }

                // We already verified that leader's port is not 0, no need for it here.
                serverStore.HasFixedPort = true;

                foreach (var url in otherNodesUrls)
                {
                    progress.AddInfo($"Adding node '{url.Key}' to the cluster.");
                    onProgress(progress);

                    try
                    {
                        await serverStore.AddNodeToClusterAsync(url.Value, url.Key, validateNotInTopology: false);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Failed to add node '{continueSetupInfo.NodeTag}' to the cluster.", e);
                    }
                }
            }

            progress.AddInfo("Registering client certificate in the local server.");
            onProgress(progress);
            var certDef = new CertificateDefinition
            {
                Name = $"{clientCert.SubjectName.Name}",
                // this does not include the private key, that is only for the client
                Certificate = Convert.ToBase64String(clientCert.Export(X509ContentType.Cert)),
                Permissions = new Dictionary<string, DatabaseAccess>(),
                SecurityClearance = SecurityClearance.ClusterAdmin,
                Thumbprint = clientCert.Thumbprint,
                PublicKeyPinningHash = clientCert.GetPublicKeyPinningHash(),
                NotAfter = clientCert.NotAfter
            };

            try
            {
                if (continueSetupInfo.NodeTag.Equals(firstNodeTag))
                {
                    var res = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(clientCert.Thumbprint, certDef, RaftIdGenerator.DontCareId));
                    await serverStore.Cluster.WaitForIndexNotification(res.Index);
                }
                else
                {
                    using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (var certificate = ctx.ReadObject(certDef.ToJson(), "Client/Certificate/Definition"))
                    using (var tx = ctx.OpenWriteTransaction())
                    {
                        serverStore.Cluster.PutLocalState(ctx, clientCert.Thumbprint, certificate, certDef);
                        tx.Commit();
                    }
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to register client certificate in the local server.", e);
            }

            if (continueSetupInfo.RegisterClientCert)
            {
                RegisterClientCertInOs(onProgress, progress, clientCert);
                progress.AddInfo("Registering admin client certificate in the OS personal store.");
                onProgress(progress);
            }

            var certPath = serverStore.Configuration.GetSetting(
                               RavenConfiguration.GetKey(x => x.Core.SetupResultingServerCertificatePath))
                           ?? Path.Combine(AppContext.BaseDirectory, certificateFileName);

            try
            {
                progress.AddInfo($"Saving server certificate at {certPath}.");
                onProgress(progress);

                await using (var certFile = SafeFileStream.Create(certPath, FileMode.Create))
                {
                    var certBytes = serverCertBytes;
                    await certFile.WriteAsync(certBytes, 0, certBytes.Length);
                    await certFile.FlushAsync();
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to save server certificate at {certPath}.", e);
            }

            try
            {
                // During setup we use the System database to store cluster configurations as well as the trusted certificates.
                // We need to make sure that the currently used data dir will be the one written (or not written) in the resulting settings.json
                var dataDirKey = RavenConfiguration.GetKey(x => x.Core.DataDirectory);
                var currentDataDir = serverStore.Configuration.GetServerWideSetting(dataDirKey) ?? serverStore.Configuration.GetSetting(dataDirKey);
                var currentHasKey = string.IsNullOrWhiteSpace(currentDataDir) == false;

                if (currentHasKey)
                {
                    settingsJsonObject.Modifications = new DynamicJsonValue(settingsJsonObject) {[dataDirKey] = currentDataDir};
                }
                else if (settingsJsonObject.TryGet(dataDirKey, out string _))
                {
                    settingsJsonObject.Modifications = new DynamicJsonValue(settingsJsonObject);
                    settingsJsonObject.Modifications.Remove(dataDirKey);
                }

                if (settingsJsonObject.Modifications != null)
                    settingsJsonObject = context.ReadObject(settingsJsonObject, "settings.json");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to determine the data directory", e);
            }

            try
            {
                progress.AddInfo($"Saving configuration at {serverStore.Configuration.ConfigPath}.");
                onProgress(progress);

                var indentedJson = LetsEncryptUtils.IndentJsonString(settingsJsonObject.ToString());
                LetsEncryptUtils.WriteSettingsJsonLocally(serverStore.Configuration.ConfigPath, indentedJson);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to save configuration at {serverStore.Configuration.ConfigPath}.", e);
            }

            try
            {
                progress.Readme = LetsEncryptUtils.CreateReadmeText(continueSetupInfo.NodeTag, publicServerUrl, true, continueSetupInfo.RegisterClientCert);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to create the readme text.", e);
            }
        }

        private static async Task<byte[]> CompleteClusterConfigurationAndGetSettingsZip(
            Action<IOperationProgress> onProgress,
            SetupProgressAndResult progress,
            SetupMode setupMode,
            SetupInfo setupInfo,
            ServerStore serverStore,
            CancellationToken token)
        {
            return await LetsEncryptUtils.CompleteClusterConfigurationAndGetSettingsZip(new LetsEncryptUtils.CompleteClusterConfigurationParameters
            {
                onProgress = onProgress,
                Progress = progress,
                SetupInfo = setupInfo,
                OnWriteSettingsJsonLocally = indentedJson  => 
                {
                   return Task.Run(()=>LetsEncryptUtils.WriteSettingsJsonLocally(serverStore.Configuration.ConfigPath, indentedJson), token);
                },
                OnGetCertificatePath = certificateFileName =>
                {
                    return Task.Run(()=>  serverStore.Configuration.GetSetting(
                                              RavenConfiguration.GetKey(x => x.Core.SetupResultingServerCertificatePath))
                                          ?? Path.Combine(AppContext.BaseDirectory, certificateFileName), token);
                },
                OnPutServerWideStudioConfigurationValues = async studioEnvironment =>
                {
                    var res = await serverStore.PutValueInClusterAsync(new PutServerWideStudioConfigurationCommand(
                        new ServerWideStudioConfiguration
                        {
                            Disabled = false,
                            Environment = studioEnvironment
                        }, RaftIdGenerator.DontCareId));
                            
                    await serverStore.Cluster.WaitForIndexNotification(res.Index);
                },
                OnBeforeAddingNodesToCluster = async (publicServerUrl, serverCert, serverCertBytes, localNodeTag) =>
                {
                    try
                    {
                        serverStore.Engine.SetNewState(RachisState.Passive, null, serverStore.Engine.CurrentTerm, "During setup wizard, " +
                            "making sure there is no cluster from previous installation.");
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Failed to delete previous cluster topology during setup.", e);
                    }

                    await serverStore.EnsureNotPassiveAsync(publicServerUrl, setupInfo.LocalNodeTag);

                    await DeleteAllExistingCertificates(serverStore);

                    if (setupMode == SetupMode.LetsEncrypt)
                    {
                        await serverStore.EnsureNotPassiveAsync(skipLicenseActivation: true);
                        await serverStore.LicenseManager.ActivateAsync(setupInfo.License, RaftIdGenerator.DontCareId);
                    }

                    serverStore.HasFixedPort = setupInfo.NodeSetupInfos[localNodeTag].Port != 0;
                },
                AddNodeToCluster = async nodeTag =>
                {
                    try
                    {
                        await serverStore.AddNodeToClusterAsync(setupInfo.NodeSetupInfos[nodeTag].PublicServerUrl, nodeTag, validateNotInTopology: false, token: token);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Failed to add node '{nodeTag}' to the cluster.", e);
                    }
                },
                RegisterClientCertInOs =  (onProgressCopy, progressCopy, clientCert) =>
                {
                    return Task.Run(()=>RegisterClientCertInOs(onProgressCopy, progressCopy, clientCert), token);
                },
            });
        }


        private static void RegisterClientCertInOs(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, X509Certificate2 clientCert)
        {
            using (var userPersonalStore = new X509Store(StoreName.My, StoreLocation.CurrentUser, OpenFlags.ReadWrite))
            {
                try
                {
                    userPersonalStore.Add(clientCert);
                    progress.AddInfo($"Successfully registered the admin client certificate in the OS Personal CurrentUser Store '{userPersonalStore.Name}'.");
                    onProgress(progress);
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Failed to register client certificate in the current user personal store '{userPersonalStore.Name}'.", e);
                }
            }
        }

        

        private class UniqueResponseResponder : IStartup
        {
            private readonly string _response;

            public UniqueResponseResponder(string response)
            {
                _response = response;
            }

            public IServiceProvider ConfigureServices(IServiceCollection services)
            {
                return services.BuildServiceProvider();
            }

            public void Configure(IApplicationBuilder app)
            {
                app.Run(async context =>
                {
                    await context.Response.WriteAsync(_response);
                });
            }
        }

        public static async Task SimulateRunningServer(ServerStore serverStore, X509Certificate2 serverCertificate, string serverUrl, string nodeTag,
            IPEndPoint[] addresses, int port, string settingsPath, SetupMode setupMode, CancellationToken token)
        {
            var configuration = RavenConfiguration.CreateForServer(null, settingsPath);
            configuration.Initialize();
            var guid = Guid.NewGuid().ToString();

            IWebHost webHost = null;
            try
            {
                try
                {
                    var responder = new UniqueResponseResponder(guid);

                    var webHostBuilder = new WebHostBuilder()
                        .CaptureStartupErrors(captureStartupErrors: true)
                        .UseKestrel(options =>
                        {
                            var httpsConnectionMiddleware = new HttpsConnectionMiddleware(serverStore.Server, options);
                            httpsConnectionMiddleware.SetCertificate(serverCertificate);

                            if (addresses.Length == 0)
                            {
                                var defaultIp = new IPEndPoint(IPAddress.Parse("0.0.0.0"), port == 0 ? 443 : port);

                                options.Listen(defaultIp, listenOptions =>
                                {
                                    listenOptions
                                        .UseHttps()
                                        .Use(httpsConnectionMiddleware.OnConnectionAsync);
                                });
                                if (Logger.IsInfoEnabled)
                                    Logger.Info($"List of ip addresses for node '{nodeTag}' is empty. WebHost listening to {defaultIp}");
                            }

                            foreach (var address in addresses)
                            {
                                options.Listen(address, listenOptions =>
                                {
                                    listenOptions
                                        .UseHttps()
                                        .Use(httpsConnectionMiddleware.OnConnectionAsync);
                                });
                            }
                        })
                        .UseSetting(WebHostDefaults.ApplicationKey, "Setup simulation")
                        .ConfigureServices(collection => { collection.AddSingleton(typeof(IStartup), responder); })
                        .UseShutdownTimeout(TimeSpan.FromMilliseconds(150));

                    webHost = webHostBuilder.Build();

                    await webHost.StartAsync(token);
                }
                catch (Exception e)
                {
                    string linuxMsg = null;
                    if (PlatformDetails.RunningOnPosix && (port == 80 || port == 443))
                    {
                        linuxMsg = $"It can happen if port '{port}' is not allowed for the non-root RavenDB process." +
                                   $"Try using setcap to allow it: sudo setcap CAP_NET_BIND_SERVICE=+eip {Path.Combine(AppContext.BaseDirectory, "Raven.Server")}";
                    }

                    var also = linuxMsg == null ? string.Empty : "also";
                    var externalIpMsg = setupMode == SetupMode.LetsEncrypt
                        ? $"It can {also} happen if the ip is external (behind a firewall, docker). If this is the case, try going back to the previous screen and add the same ip as an external ip."
                        : string.Empty;

                    throw new InvalidOperationException(
                        $"Failed to start WebHost on node '{nodeTag}'. The specified ip address might not be reachable due to network issues. {linuxMsg}{Environment.NewLine}{externalIpMsg}{Environment.NewLine}" +
                        $"Settings file:{settingsPath}.{Environment.NewLine}" +
                        $"IP addresses: {string.Join(", ", addresses.Select(address => address.ToString()))}.", e);
                }

                using (var httpMessageHandler = new HttpClientHandler())
                {
                    // on MacOS this is not supported because Apple...
                    if (PlatformDetails.RunningOnMacOsx == false)
                    {
                        httpMessageHandler.ServerCertificateCustomValidationCallback += (message, certificate2, chain, errors) =>
                            // we want to verify that we get the same thing back
                        {
                            if (certificate2.Thumbprint != serverCertificate.Thumbprint)
                                throw new InvalidOperationException("Expected to get " + serverCertificate.FriendlyName + " with thumbprint " +
                                                                    serverCertificate.Thumbprint + " but got " +
                                                                    certificate2.FriendlyName + " with thumbprint " + certificate2.Thumbprint);
                            return true;
                        };
                    }

                    using (var client = new HttpClient(httpMessageHandler) {BaseAddress = new Uri(serverUrl),})
                    {
                        HttpResponseMessage response = null;
                        string result = null;
                        try
                        {
                            using (var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
                            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, cancellationTokenSource.Token))
                            {
                                response = await client.GetAsync("/are-you-there?", cts.Token);
                                response.EnsureSuccessStatusCode();
                                result = await response.Content.ReadAsStringAsync();
                                if (result != guid)
                                {
                                    throw new InvalidOperationException($"Expected result guid: {guid} but got {result}.");
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            if (setupMode == SetupMode.Secured && await CanResolveHostNameLocally(serverUrl, addresses) == false)
                            {
                                throw new InvalidOperationException(
                                    $"Failed to resolve '{serverUrl}'. Try to clear your local/network DNS cache and restart validation.", e);
                            }

                            throw new InvalidOperationException($"Client failed to contact WebHost listening to '{serverUrl}'.{Environment.NewLine}" +
                                                                $"Are you blocked by a firewall? Make sure the port is open.{Environment.NewLine}" +
                                                                $"Settings file:{settingsPath}.{Environment.NewLine}" +
                                                                $"IP addresses: {string.Join(", ", addresses.Select(address => address.ToString()))}.{Environment.NewLine}" +
                                                                $"Response: {response?.StatusCode}.{Environment.NewLine}{result}", e);
                        }
                    }
                }
            }
            finally
            {
                if (webHost != null)
                    await webHost.StopAsync(TimeSpan.Zero);
            }
        }

        private static async Task<bool> CanResolveHostNameLocally(string serverUrl, IPEndPoint[] expectedAddresses)
        {
            var expectedIps = expectedAddresses.Select(address => address.Address.ToString()).ToHashSet();
            var hostname = new Uri(serverUrl).Host;
            HashSet<string> actualIps;

            try
            {
                actualIps = (await Dns.GetHostAddressesAsync(hostname)).Select(address => address.ToString()).ToHashSet();
            }
            catch (Exception)
            {
                return false;
            }

            return expectedIps.SetEquals(actualIps);
        }

        private static async Task AssertDnsUpdatedSuccessfully(string serverUrl, IPEndPoint[] expectedAddresses, CancellationToken token)
        {
            // First we'll try to resolve the hostname through google's public dns api
            using (var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, cancellationTokenSource.Token))
            {
                var expectedIps = expectedAddresses.Select(address => address.Address.ToString()).ToHashSet();

                var hostname = new Uri(serverUrl).Host;

                using (var client = new HttpClient {BaseAddress = new Uri(GoogleDnsApi)})
                {
                    var response = await client.GetAsync($"/resolve?name={hostname}", cts.Token);

                    var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                    if (response.IsSuccessStatusCode == false)
                        throw new InvalidOperationException($"Tried to resolve '{hostname}' using Google's api ({GoogleDnsApi}).{Environment.NewLine}"
                                                            + $"Request failed with status {response.StatusCode}.{Environment.NewLine}{responseString}");

                    dynamic dnsResult = JsonConvert.DeserializeObject(responseString);

                    // DNS response format: https://developers.google.com/speed/public-dns/docs/dns-over-https

                    if (dnsResult.Status != 0)
                        throw new InvalidOperationException($"Tried to resolve '{hostname}' using Google's api ({GoogleDnsApi}).{Environment.NewLine}"
                                                            + $"Got a DNS failure response:{Environment.NewLine}{responseString}" +
                                                            Environment.NewLine +
                                                            "Please wait a while until DNS propagation is finished and try again. If you are trying to update existing DNS records, it might take hours to update because of DNS caching. If the issue persists, contact RavenDB's support.");

                    JArray answers = dnsResult.Answer;
                    var googleIps = answers.Select(answer => answer["data"].ToString()).ToHashSet();

                    if (googleIps.SetEquals(expectedIps) == false)
                        throw new InvalidOperationException($"Tried to resolve '{hostname}' using Google's api ({GoogleDnsApi}).{Environment.NewLine}"
                                                            + $"Expected to get these ips: {string.Join(", ", expectedIps)} while Google's actual result was: {string.Join(", ", googleIps)}"
                                                            + Environment.NewLine +
                                                            "Please wait a while until DNS propagation is finished and try again. If you are trying to update existing DNS records, it might take hours to update because of DNS caching. If the issue persists, contact RavenDB's support.");
                }

                // Resolving through google worked, now let's check locally
                HashSet<string> actualIps;
                try
                {
                    actualIps = (await Dns.GetHostAddressesAsync(hostname)).Select(address => address.ToString()).ToHashSet();
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException(
                        $"Cannot resolve '{hostname}' locally but succeeded resolving the address using Google's api ({GoogleDnsApi})."
                        + Environment.NewLine + "Try to clear your local/network DNS cache or wait a few minutes and try again."
                        + Environment.NewLine + "Another temporary solution is to configure your local network connection to use Google's DNS server (8.8.8.8).", e);
                }

                if (expectedIps.SetEquals(actualIps) == false)
                    throw new InvalidOperationException(
                        $"Tried to resolve '{hostname}' locally but got an outdated result."
                        + Environment.NewLine + $"Expected to get these ips: {string.Join(", ", expectedIps)} while the actual result was: {string.Join(", ", actualIps)}"
                        + Environment.NewLine + $"If we try resolving through Google's api ({GoogleDnsApi}), it works well."
                        + Environment.NewLine + "Try to clear your local/network DNS cache or wait a few minutes and try again."
                        + Environment.NewLine + "Another temporary solution is to configure your local network connection to use Google's DNS server (8.8.8.8).");
            }
        }

        // Duplicate of AdminCertificatesHandler.GenerateCertificateInternal stripped from authz checks, used by an unauthenticated client during setup only
        // public static async Task<byte[]> GenerateCertificateTask2(string name, RavenServer.CertificateHolder serverCertificateHolder, SetupInfo setupInfo)
        // {
        //     if (serverStore.Server.Certificate?.Certificate == null)
        //         throw new InvalidOperationException($"Cannot generate the client certificate '{name}' because the server certificate is not loaded.");
        //
        //     // this creates a client certificate which is signed by the current server certificate
        //     var selfSignedCertificate = CertificateUtils.CreateSelfSignedClientCertificate(name, serverStore.Server.Certificate, out var certBytes,
        //         setupInfo.ClientCertNotAfter ?? DateTime.UtcNow.Date.AddYears(5));
        //
        //     var newCertDef = new CertificateDefinition
        //     {
        //         Name = name,
        //         // this does not include the private key, that is only for the client
        //         Certificate = Convert.ToBase64String(selfSignedCertificate.Export(X509ContentType.Cert)),
        //         Permissions = new Dictionary<string, DatabaseAccess>(),
        //         SecurityClearance = SecurityClearance.ClusterAdmin,
        //         Thumbprint = selfSignedCertificate.Thumbprint,
        //         PublicKeyPinningHash = selfSignedCertificate.GetPublicKeyPinningHash(),
        //         NotAfter = selfSignedCertificate.NotAfter
        //     };
        //
        //     var res = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(selfSignedCertificate.Thumbprint, newCertDef, RaftIdGenerator.DontCareId));
        //     await serverStore.Cluster.WaitForIndexNotification(res.Index);
        //
        //     return certBytes;
        // }
        //
        public static async Task<byte[]> GenerateCertificateTask(string name, ServerStore serverStore, SetupInfo setupInfo)
        {
            if (serverStore.Server.Certificate?.Certificate == null)
                throw new InvalidOperationException($"Cannot generate the client certificate '{name}' because the server certificate is not loaded.");

            // this creates a client certificate which is signed by the current server certificate
            var selfSignedCertificate = CertificateUtils.CreateSelfSignedClientCertificate(name, serverStore.Server.Certificate, out var certBytes,
                setupInfo.ClientCertNotAfter ?? DateTime.UtcNow.Date.AddYears(5));

            var newCertDef = new CertificateDefinition
            {
                Name = name,
                // this does not include the private key, that is only for the client
                Certificate = Convert.ToBase64String(selfSignedCertificate.Export(X509ContentType.Cert)),
                Permissions = new Dictionary<string, DatabaseAccess>(),
                SecurityClearance = SecurityClearance.ClusterAdmin,
                Thumbprint = selfSignedCertificate.Thumbprint,
                PublicKeyPinningHash = selfSignedCertificate.GetPublicKeyPinningHash(),
                NotAfter = selfSignedCertificate.NotAfter
            };

            var res = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(selfSignedCertificate.Thumbprint, newCertDef, RaftIdGenerator.DontCareId));
            await serverStore.Cluster.WaitForIndexNotification(res.Index);

            return certBytes;
        }
    }
}
