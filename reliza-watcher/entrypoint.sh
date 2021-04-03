#!/bin/bash
images_new=" "
# cache to limit api usage
images_old=" "
while [ true ]
do
    cp /resources/images /resources/images_old
    kubectl get po -o json | jq "[{namespace:.items[].metadata.namespace, pod:.items[].metadata.name, status:.items[].status.containerStatuses[]}] > /resources/images
    difflines=$(diff /resources/images /resources/images_old | wc -l)
    if [ $difflines -gt 0 ]
    then
        echo "$(date) - change in images detected - shipping images to Reliza Hub"
        echo "new images = $images_new"
        if [ "$NAMESPACE" == "allnamespaces" ]
        then
            readarray -t NAMESPACES < <(kubectl get ns -o custom-columns=NAME:.metadata.name)
        else
            IFS="," read -ra NAMESPACES <<< "$NAMESPACE"
        fi
        for ns in "${NAMESPACES[@]}"; do
            if [ $ns != "NAME" ]
            then
                echo "$(date) shipping images for ns $ns"
                /app/app instdata -u $HUB_URI -i $RELIZA_API_ID -k $RELIZA_API_KEY --sender $SENDER_ID --namespace $ns --imagestyle k8s --imagefile /resources/images
            fi
        done
    fi
    sleep 10
done