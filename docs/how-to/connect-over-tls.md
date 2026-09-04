# Connect over TLS

Plaintext exposes the `HELLO ... AUTH` handshake, so any connection crossing a network you do not
control needs TLS.

## Enable TLS with platform validation

```csharp
await using var valkey = await ValkeyClient.ConnectAsync(
    new ValkeyClientOptions
    {
        Host = "valkey.example.com",
        Port = 6379,
        UseTls = true,
        Username = "app",
        Password = Environment.GetEnvironmentVariable("VALKEY_PASSWORD"),
    }
);
```

This is the configuration you want in production. With `CertificateValidationCallback` left `null`,
the platform validates the chain and the host name against `Host`.

## Keep credentials out of source

Read them from configuration, environment, or a secret store. A password in a literal ends up in
source control, crash dumps, and CI logs:

```csharp
Password = builder.Configuration["Valkey:Password"],
```

`Username` without `Password` throws `ArgumentException` at connect time rather than silently
authenticating as the default user.

## Trust a private CA

Point the platform at your CA rather than disabling validation. Install the CA in the OS trust store,
or validate the chain explicitly against it:

```csharp
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

var privateCa = X509CertificateLoader.LoadCertificateFromFile("private-ca.crt");

var options = new ValkeyClientOptions
{
    Host = "valkey.internal",
    UseTls = true,
    CertificateValidationCallback = (_, certificate, chain, errors) =>
    {
        if (errors == SslPolicyErrors.None)
            return true;
        if (errors != SslPolicyErrors.RemoteCertificateChainErrors || certificate is null || chain is null)
            return false;

        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(privateCa);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        return chain.Build(new X509Certificate2(certificate));
    },
};
```

Note what this still enforces: a host-name mismatch produces `RemoteCertificateNameMismatch` and is
rejected. Only chain errors are re-evaluated, and only against your CA.

> **A supplied callback replaces platform validation entirely.** The platform result is not consulted
> afterwards. Whatever your callback returns is the decision, so it must reject everything it has not
> positively verified.

## What not to do

```csharp
// Never. This accepts any certificate from any party on the network path,
// which removes every guarantee TLS provides.
CertificateValidationCallback = (_, _, _, _) => true,
```

A callback that returns `true` unconditionally turns TLS into unauthenticated encryption: an attacker
who can intercept the connection presents their own certificate, you accept it, and they read the
`AUTH` handshake in clear. Do not use this even in a test helper — use the custom-trust-store pattern
above with the test CA instead.

## Verify the connection

```csharp
Console.WriteLine(valkey.NegotiatedProtocol);
Console.WriteLine(await valkey.PingAsync());
```

If the TLS handshake fails, `ConnectAsync` throws before either line runs. A handshake that exceeds
`ConnectTimeout` (5 seconds by default) throws `TimeoutException`.

## Related

- [`ValkeyClientOptions`](../reference/client-options.md) — every setting and its default.
- [Handle errors](handle-errors.md) — which failures are recoverable.
