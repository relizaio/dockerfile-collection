#!/bin/bash
images_new=" "
# cache to limit api usage
images_old=" "
while [ true ]
do
    images_new=$(kubectl get pods --all-namespaces -o jsonpath="{.items[*].status.containerStatuses[0].imageID}")
    if [ "$images_new" != "$images_old" ]
    then
        images_old="$images_new"
        NAMESPACES=""
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
                images=$(kubectl get pods -n $ns -o jsonpath="{.items[*].status.containerStatuses[0].imageID}")
                echo "$(date) shipping images for ns $ns = $images"
                /app/app instdata -u $HUB_URI -i $RELIZA_API_ID -k $RELIZA_API_KEY --sender $SENDER_ID --namespace $ns --images "$images"
            fi
        done
    fi
    sleep 10
done