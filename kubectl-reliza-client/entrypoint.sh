images=" "
if [ $NAMESPACE == "allnamespaces" ]
then
    images=$(kubectl get pods --all-namespaces -o jsonpath="{.items[*].status.containerStatuses[0].imageID}")
else
    images=$(kubectl get pods -n $NAMESPACE -o jsonpath="{.items[*].status.containerStatuses[0].imageID}")
fi
/app/app instdata -u $HUB_URI -i $RELIZA_API_ID -k $RELIZA_API_KEY --images "$images"