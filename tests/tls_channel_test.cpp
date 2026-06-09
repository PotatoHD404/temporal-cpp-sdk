// Exercises the SDK's TLS / mTLS / API-key code path against an in-process gRPC
// server that speaks TLS — no external Temporal server needed, so this runs in
// CI. It proves Client TLS options negotiate a TLS handshake, that mTLS client
// certs are presented, and that the api_key is sent as an Authorization header.
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/security/server_credentials.h>

#include "temporal/api/workflowservice/v1/request_response.pb.h"
#include "temporal/api/workflowservice/v1/service.grpc.pb.h"

#include <temporal/client/client.h>
#include <temporal/common/options.h>

namespace {

namespace wsv = ::temporal::api::workflowservice::v1;

// Self-signed cert (CN=localhost, SAN DNS:localhost/IP:127.0.0.1, ~100y) used as
// both the server cert and, for mTLS, the client cert and trust root.
constexpr char kCertPem[] = R"(-----BEGIN CERTIFICATE-----
MIIDJzCCAg+gAwIBAgIUNB9NQj+RNrKrfz2QbEvGgj8Hg6cwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MDYwOTEwMTc0OVoYDzIxMjYw
NTE2MTAxNzQ5WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQDdgbNn/Tpi1hC/OQg1yvGdyDgpxOMOPPB9vT7M3po6
aYCVSZJOtMaIpxj3pWw1da9Q3XxhvYjz1w/8Qrkp3paHvBJK5BfTN0NRCzArZPiD
donf3x3TtCw9bUpvduUVHXwxIinxN0PKiYQEoWTUdwzz0sOCepgg0HYG8GVZnXBf
YvSrzuRB39jHtJ0qsOv+9owUu6b1azhwlXErjb1lMMI5RAD0gMOeOoKB7yRaaBqu
BSq5QCYSQISwYIII6ujCul4n3pzBMwEN1s3Pui45aSiUlOkb/QFMavgNL46uy955
dao5VbUA5ZhvarovAyMQogM+CxTVPyx5rOWDdPuV+PdRAgMBAAGjbzBtMB0GA1Ud
DgQWBBSbdVFos+gHyYct203kyun3gnUC8jAfBgNVHSMEGDAWgBSbdVFos+gHyYct
203kyun3gnUC8jAPBgNVHRMBAf8EBTADAQH/MBoGA1UdEQQTMBGCCWxvY2FsaG9z
dIcEfwAAATANBgkqhkiG9w0BAQsFAAOCAQEAswmh+g7YaZLQwU9aF5siYaw8aXHv
KX5XiPlTKhgeF/vCLXiWpxLMA0Z/qDjm8HfpKYONDeiSA5ObHMeWQvEGRRYL3i/J
pXjPZwMlPiR4xdLPDfF0L/xBlquhMVIsQ0ncN2RZ1sQUXRNRkgyJb3tk52xVuCRX
sqx6yazq7lhAQwsu3D4oCgRRNs0rfvKDDAPoDXCkIYQHVWzfH7qBFtEhhMew3i6Q
6S8i2zCEZUnDKO8T8CUQCcWPkakr1tCDKKR99vNsCTRK0D6fqQftJp53LUmShHDg
N+geYvBPkyH6GuefzDsbs1gAHpSm/ofoIMWs2/wkqpLl7hBipdm+iUyfJA==
-----END CERTIFICATE-----
)";

constexpr char kKeyPem[] = R"(-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDdgbNn/Tpi1hC/
OQg1yvGdyDgpxOMOPPB9vT7M3po6aYCVSZJOtMaIpxj3pWw1da9Q3XxhvYjz1w/8
Qrkp3paHvBJK5BfTN0NRCzArZPiDdonf3x3TtCw9bUpvduUVHXwxIinxN0PKiYQE
oWTUdwzz0sOCepgg0HYG8GVZnXBfYvSrzuRB39jHtJ0qsOv+9owUu6b1azhwlXEr
jb1lMMI5RAD0gMOeOoKB7yRaaBquBSq5QCYSQISwYIII6ujCul4n3pzBMwEN1s3P
ui45aSiUlOkb/QFMavgNL46uy955dao5VbUA5ZhvarovAyMQogM+CxTVPyx5rOWD
dPuV+PdRAgMBAAECgf9jnJAxk49OIzYkyGEIz9sYOZIINqJlJMKByTkUqIZy/j70
48EH5APkMEDqHVosMGBG3VOyIKoV/gESUG80hQTsxYb0Zt7P/WooZ/+hhCa99/DI
3G1tZvj+JsfymGUsubW/4r053MB/tJfJ/Up6wY7xlzaU1szS9Owe8ryhMEv4mJFX
KmqKB2mTkPL0qPQ1CYoNrxt50w5mAC6+ejU4TohBjrJjQUTqobkD439bRJrWE9UW
Fpdlp+ch3om5C/1+59NDETFUjOu1w12wK/zrzMCQOkfHFTK0AMSdU1t2pffIUKsT
DfqsfocKualSw9wmARPp+kN/mffs7jbalkCJ2akCgYEA97bHxGYKrUf7JLn++AA0
Hcj3ECZz7rcw3W0ow4XFakc2x3w1hKCsm5JKliwuOypa/tiiaw+QPNWBEwIiMrYW
Xa2PeVIZ17atL0HT8w7dbO1kPQuzHGMTUwwNsdjqveYPl4ErUph2YoymB/nAFW/q
xtc05cQDwOzqI3C+EGRlKfkCgYEA5OqAk94tqyAnfEhtOGP0v9Bp5R2AhzagKHaa
c3U029YOcOfc01Ephv3ZVuCkI6kPzqzQetWqVRyzIzeEIzFI85jYjQInVcpibpNf
3QJvjUKdJT3mcF3qeIwubvRxQzJRVfP8rg+L8tFANq+6KwF4MS/0pGtQEAqfbKnP
a+M+ThkCgYEA7cy35aCX61VIkS9Ex0tavKUqGITxkl6mOEsMcPbAV5BZ1BM3RUUB
rq83jwaGsyGsDS5mbSSZsOy9ZkQMFGac/f0Z2LuqN10U0GL/VzwT8PfL3JaYsU2j
RXwywWKdpwNuQGEt97KJI34l/U4SygGQfqYmD9SmTdShyLf4nb/jJfECgYBr8gjf
sY1nfKoh+SVHyhrHuMe2usq4+BFeA0+h0ksyvyXgJ/YBz+v9NAcg6J1+E2LY2rUU
t1yy9e2jVbKBxePYuuKi27kgw2bXLbeuyE9CFX906FOZ+S9v2Oqsd6hRP5ELLxqg
GcSso+/b2dG4JeE/kJWUUuZWKiwzWX/uKCJhQQKBgQDCWZofWUHj9tMwTg+7UB9E
3HK6+vBlJ+VxFuyWd0LJQTuwB/d0gpPhhqFAYG10magOMDyXHDw+kBTGqnXBXpvf
LhVg0uwjrl/BLl3pJ8JRvTFho+K4V8ptXpFKUn/QP6XyHP7NsDn4QPYrDXjWmd94
0z7iSCRaW54D7ySwS/oCBw==
-----END PRIVATE KEY-----
)";

// Minimal WorkflowService that captures the authorization metadata and peer cert
// of the GetClusterInfo call (issued by Client::DescribeCluster) and returns a
// known cluster name. All other RPCs stay UNIMPLEMENTED.
class CapturingService final : public wsv::WorkflowService::Service {
 public:
  grpc::Status GetClusterInfo(grpc::ServerContext* ctx, const wsv::GetClusterInfoRequest*,
                              wsv::GetClusterInfoResponse* resp) override {
    const auto it = ctx->client_metadata().find("authorization");
    if (it != ctx->client_metadata().end()) {
      captured_authorization.assign(it->second.data(), it->second.size());
    }
    saw_peer_identity = !ctx->auth_context()->GetPeerIdentity().empty();
    resp->set_cluster_name("tls-test-cluster");
    return grpc::Status::OK;
  }
  std::string captured_authorization;
  bool saw_peer_identity = false;
};

struct TlsServer {
  CapturingService service;
  std::unique_ptr<grpc::Server> server;
  int port = 0;

  void Start(bool require_client_cert) {
    grpc::SslServerCredentialsOptions ssl(
        require_client_cert ? GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY
                            : GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE);
    ssl.pem_key_cert_pairs.push_back({kKeyPem, kCertPem});
    if (require_client_cert) {
      ssl.pem_root_certs = kCertPem;  // verify the client cert against our CA
    }
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::SslServerCredentials(ssl), &port);
    builder.RegisterService(&service);
    server = builder.BuildAndStart();
  }
  ~TlsServer() {
    if (server) {
      server->Shutdown();
    }
  }
};

temporal::ClientOptions TlsClientOptions(int port, bool mtls) {
  temporal::ClientOptions o;
  o.target = "127.0.0.1:" + std::to_string(port);
  o.tls.enabled = true;
  o.tls.server_ca_cert = kCertPem;  // self-signed: trust the cert directly
  o.tls.server_name = "localhost";  // SNI / cert-name override (cert CN/SAN)
  o.api_key = "secret-token";
  if (mtls) {
    o.tls.client_cert = kCertPem;
    o.tls.client_key = kKeyPem;
  }
  return o;
}

// Server TLS + API key: the RPC must complete over TLS and the api_key must
// arrive as an Authorization: Bearer header.
TEST(TlsChannel, ServerTlsCarriesApiKey) {
  TlsServer srv;
  srv.Start(/*require_client_cert=*/false);
  ASSERT_GT(srv.port, 0);
  auto client = temporal::client::Client::Connect(TlsClientOptions(srv.port, /*mtls=*/false));
  const auto desc = client.DescribeCluster();
  EXPECT_EQ(desc.cluster_name, "tls-test-cluster");  // handshake + RPC succeeded
  EXPECT_EQ(srv.service.captured_authorization, "Bearer secret-token");
}

// Mutual TLS: the server requires + verifies a client cert; the client presents
// one and the call still succeeds.
TEST(TlsChannel, MutualTlsPresentsClientCert) {
  TlsServer srv;
  srv.Start(/*require_client_cert=*/true);
  ASSERT_GT(srv.port, 0);
  auto client = temporal::client::Client::Connect(TlsClientOptions(srv.port, /*mtls=*/true));
  const auto desc = client.DescribeCluster();
  EXPECT_EQ(desc.cluster_name, "tls-test-cluster");
  EXPECT_TRUE(srv.service.saw_peer_identity);  // client cert presented + verified
}

}  // namespace
