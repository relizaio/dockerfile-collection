#!/bin/bash
# cache to limit api usage

record_timestamp=0

send_data () {
    echo "$(date) - change in images detected - shipping images to Reliza Hub"
    if [ "$NAMESPACE" == "allnamespaces" ]
    then
        readarray -t NAMESPACES < <(kubectl get ns -o custom-columns=NAME:.metadata.name)
    else
        IFS="," read -ra NAMESPACES <<< "$NAMESPACE"
    fi
    relizaHubUri=https://app.relizahub.com
    if [ ! -z "$HUB_URI" ]
    then
      relizaHubUri=$HUB_URI
    fi
    for ns in "${NAMESPACES[@]}"; do
        if [ $ns != "NAME" ]
        then
            kubectl get po -n $ns -o json | jq "[.items[] | {namespace:.metadata.namespace, labels:.metadata.labels, annotations:.metadata.annotations, pod:.metadata.name, status:.status.containerStatuses[]}]" > /resources/images_to_send
            echo "$(date) shipping images for $ns namespace"
            
            /app/app instdata -u $relizaHubUri -i $RELIZA_API_ID -k $RELIZA_API_KEY --sender $SENDER_ID$ns --namespace $ns --imagestyle k8s --imagefile /resources/images_to_send
            
            # record last sent timestamp
            if [ $record_timestamp -eq 1 ]
            then
                date +"%s" > /resources/last_sent
            fi
        fi
    done
}

date +"%s" > /resources/last_sent
while [ true ]
do
    record_timestamp=0
    cp /resources/images /resources/images_old
    if [ "$NAMESPACE" == "allnamespaces" ]
    then
      kubectl get po --all-namespaces -o json | jq "[.items[] | {namespace:.metadata.namespace, labels:.metadata.labels, annotations:.metadata.annotations, pod:.metadata.name, status:.status.containerStatuses[]}]" > /resources/images
    else
      echo "images start" > /resources/images
      IFS="," read -ra NAMESPACESCHECK <<< "$NAMESPACE"
      for nsc in "${NAMESPACESCHECK[@]}"; do
        if [ $nsc != "NAME" ]
        then
            kubectl get po -n $nsc -o json | jq "[.items[] | {namespace:.metadata.namespace, labels:.metadata.labels, annotations:.metadata.annotations, pod:.metadata.name, status:.status.containerStatuses[]}]" >> /resources/images
        fi
      done
    fi
    difflines=$(diff /resources/images /resources/images_old | wc -l)
    if [ $difflines -gt 0 ]
    then
        record_timestamp=1
        send_data
    # else
        # send follow ups to ensure we converge properly
    #    record_timestamp=0
    #    if [ $(expr $(date +"%s") - $(cat /resources/last_sent)) -lt 30 ]
    #    then
    #        send_data
    #    fi
    fi
    sleep 30
done