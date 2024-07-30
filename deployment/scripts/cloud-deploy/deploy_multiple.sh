# bandwidth=(5)
# # bandwidth=(300 200 20)

# len=${#bandwidth[@]}

echo "bash scripts/cloud-deploy/deploy-cloud-LAN.sh -i -r -k"
bash scripts/cloud-deploy/deploy-cloud-LAN.sh -i -r -k
echo '=============================='

for ((i=1;i<=2;i++)); do
    echo $i
    echo "cp scripts/experiment-configuration/generate-config-$i.sh scripts/experiment-configuration/generate-config.sh"
    cp scripts/experiment-configuration/generate-config-$i.sh scripts/experiment-configuration/generate-config.sh
    echo "bash scripts/cloud-deploy/deploy-cloud-LAN.sh -d"
    bash scripts/cloud-deploy/deploy-cloud-LAN.sh -d
    echo '=============================='
done

echo "bash scripts/cloud-deploy/deploy-cloud-LAN.sh -sd"
bash scripts/cloud-deploy/deploy-cloud-LAN.sh -sd
