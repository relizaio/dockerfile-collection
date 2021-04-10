#!/bin/bash
# cache to limit api usage
while [ true ]
do
    cp /resources/images /resources/images_old
    kubectl get po --all-namespaces -o json | jq "[.items[] | {namespace:.metadata.namespace, pod:.metadata.name, status:.status.containerStatuses}]" > /resources/images
    difflines=$(diff /resources/images /resources/images_old | wc -l)
    if [ $difflines -gt 0 ]
    then
        echo "$(date) - change in images detected - shipping images to Reliza Hub"
        if [ "$NAMESPACE" == "allnamespaces" ]
        then
            readarray -t NAMESPACES < <(kubectl get ns -o custom-columns=NAME:.metadata.name)
        else
            IFS="," read -ra NAMESPACES <<< "$NAMESPACE"
        fi
        for ns in "${NAMESPACES[@]}"; do
            if [ $ns != "NAME" ]
            then
                kubectl get po -n $ns -o json | jq "[.items[] | {namespace:.metadata.namespace, pod:.metadata.name, status:.status.containerStatuses[]}]" > /resources/images_to_send
                echo "$(date) shipping images for $ns namespace"
                /app/app instdata -u $HUB_URI -i $RELIZA_API_ID -k $RELIZA_API_KEY --sender $SENDER_ID$ns --namespace $ns --imagestyle k8s --imagefile /resources/images_to_send
            fi
        done
    fi
    sleep 10
done