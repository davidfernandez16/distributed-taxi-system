[CmdletBinding()]
param()

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$secretsPath = Join-Path $projectRoot "secrets"

if (Test-Path -LiteralPath $secretsPath) {
    Write-Error "La carpeta secrets ya existe. No se sobrescribirán certificados ni claves existentes."
    exit 1
}

$certificateCommands = @'
set -eu

apk add --no-cache openssl
mkdir -p secrets/ca secrets/registry secrets/taxi-1

openssl genrsa -out secrets/ca/ca.key 4096
openssl req -x509 -new -key secrets/ca/ca.key -sha256 -days 3650 \
  -out secrets/ca/ca.crt \
  -subj '/C=ES/O=EasyCab/OU=Development/CN=EasyCab Development CA'

openssl genrsa -out secrets/registry/registry.key 2048
openssl req -new -key secrets/registry/registry.key \
  -out /tmp/registry.csr \
  -subj '/C=ES/O=EasyCab/OU=Development/CN=registry' \
  -addext 'subjectAltName=DNS:registry,DNS:localhost' \
  -addext 'extendedKeyUsage=serverAuth' \
  -addext 'keyUsage=critical,digitalSignature,keyEncipherment'
openssl x509 -req -in /tmp/registry.csr \
  -CA secrets/ca/ca.crt -CAkey secrets/ca/ca.key -CAcreateserial \
  -out secrets/registry/registry.crt -days 825 -sha256 -copy_extensions copy

openssl genrsa -out secrets/taxi-1/taxi-1.key 2048
openssl req -new -key secrets/taxi-1/taxi-1.key \
  -out /tmp/taxi-1.csr \
  -subj '/C=ES/O=EasyCab/OU=Development/CN=taxi-1' \
  -addext 'extendedKeyUsage=clientAuth' \
  -addext 'keyUsage=critical,digitalSignature,keyEncipherment'
openssl x509 -req -in /tmp/taxi-1.csr \
  -CA secrets/ca/ca.crt -CAkey secrets/ca/ca.key -CAcreateserial \
  -out secrets/taxi-1/taxi-1.crt -days 825 -sha256 -copy_extensions copy

openssl rand -out secrets/taxi-1/message.key 32
rm -f secrets/ca/ca.srl
chmod 600 secrets/ca/ca.key secrets/registry/registry.key secrets/taxi-1/taxi-1.key secrets/taxi-1/message.key
'@

docker run --rm -v "${projectRoot}:/work" -w /work alpine:3.20 sh -ceu $certificateCommands

if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host "Certificados de desarrollo creados en secrets/."
