# bandwidth=(5)
# # bandwidth=(300 200 20)

# len=${#bandwidth[@]}

# echo "bash scripts/cloud-deploy/deploy-cloud-WAN.sh -i -r -k"
# bash scripts/cloud-deploy/deploy-cloud-WAN.sh -i -r -k
echo '=============================='

for ((i=1;i<=4;i++)); do
    echo $i
    echo "cp scripts/experiment-configuration/generate-config-$i.sh scripts/experiment-configuration/generate-config.sh"
    cp scripts/experiment-configuration/generate-config-$i.sh scripts/experiment-configuration/generate-config.sh
    echo "bash scripts/cloud-deploy/deploy-cloud-WAN.sh -d"
    bash scripts/cloud-deploy/deploy-cloud-WAN.sh -d
    echo '=============================='
done

echo "bash scripts/cloud-deploy/deploy-cloud-WAN.sh -sd"
bash scripts/cloud-deploy/deploy-cloud-WAN.sh -sd
