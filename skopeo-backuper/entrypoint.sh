#!/bin/sh
set -o errexit
set -o nounset
set -o pipefail

TIMESTAMP=$(date +"%Y-%m-%d-%H-%M")
AUTH="${AUTH_FILE:-/auth/config.json}"
TAR="/tmp/backup-bundle.tar"

mkdir -p /auth

echo "Step 1: Extracting images from namespace ${K8S_NAMESPACE}"
kubectl get po -n "${K8S_NAMESPACE}" -o yaml | grep 'imageID:' | awk '{print $2}' | grep -v '^sha256:' | grep -v '^$' | sed '/^[[:space:]]*$/d' | sort -u > /tmp/images.txt

if [ ! -s /tmp/images.txt ]; then
    echo "Error: No images found in namespace ${K8S_NAMESPACE}"
    exit 1
fi

echo "Step 2: Found $(wc -l < /tmp/images.txt) unique images to backup"
cat /tmp/images.txt

echo "Step 3: Copying images to individual archives"
BACKUP_DIR="/tmp/image-backups"
rm -rf "$BACKUP_DIR"
mkdir -p "$BACKUP_DIR"
COUNT=0
while IFS= read -r img; do
    # Skip empty lines
    [ -z "$img" ] && continue
    
    echo "Backing up: $img"
    # Strip both digest and tag, add uniform :backup tag for docker-archive compatibility
    img_name=$(echo "$img" | sed -e 's/@sha256:.*//' -e 's/:[^:]*$//')
    COUNT=$((COUNT + 1))
    skopeo copy --src-authfile "$AUTH" \
        "docker://$img" "docker-archive:${BACKUP_DIR}/${COUNT}.tar:${img_name}:backup"
done < /tmp/images.txt

echo "Step 4: Bundling into single archive"
tar -cf "$TAR" -C "$BACKUP_DIR" .

echo "Step 5: Compressing backup"
gzip -f "$TAR"
BACKUP_FILE="${TAR}.gz"

echo "Step 6: Creating image list artifact"
IMAGE_LIST="/tmp/image-list-${TIMESTAMP}.txt"
cp /tmp/images.txt "${IMAGE_LIST}"

echo "Step 7: Encrypting and uploading"
if [ -n "${ENCRYPTION_PASSWORD:-}" ]; then
    openssl enc -aes-256-cbc -a -pbkdf2 -iter 600000 -salt -pass pass:"${ENCRYPTION_PASSWORD}" \
        -in "${BACKUP_FILE}" -out "${BACKUP_FILE}.enc"
    UPLOAD_FILE="${BACKUP_FILE}.enc"
    
    openssl enc -aes-256-cbc -a -pbkdf2 -iter 600000 -salt -pass pass:"${ENCRYPTION_PASSWORD}" \
        -in "${IMAGE_LIST}" -out "${IMAGE_LIST}.enc"
    UPLOAD_LIST="${IMAGE_LIST}.enc"
else
    UPLOAD_FILE="${BACKUP_FILE}"
    UPLOAD_LIST="${IMAGE_LIST}"
fi

if [ -n "${AWS_BUCKET:-}" ]; then
    aws s3 cp "${UPLOAD_FILE}" "s3://${AWS_BUCKET}/${BACKUP_PREFIX:-backup}-${TIMESTAMP}.tar.gz${ENCRYPTION_PASSWORD:+.enc}"
    echo "Uploaded to S3: s3://${AWS_BUCKET}/${BACKUP_PREFIX:-backup}-${TIMESTAMP}.tar.gz${ENCRYPTION_PASSWORD:+.enc}"
    
    aws s3 cp "${UPLOAD_LIST}" "s3://${AWS_BUCKET}/${BACKUP_PREFIX:-backup}-${TIMESTAMP}-images.txt${ENCRYPTION_PASSWORD:+.enc}"
    echo "Uploaded image list to S3: s3://${AWS_BUCKET}/${BACKUP_PREFIX:-backup}-${TIMESTAMP}-images.txt${ENCRYPTION_PASSWORD:+.enc}"
fi

echo "Backup completed: ${UPLOAD_FILE}"
echo "Image list: ${UPLOAD_LIST}"
