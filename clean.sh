docker stop $(docker ps -aq) 2>/dev/null
docker rm -f $(docker ps -aq) 2>/dev/null
docker rmi -f $(docker images -aq) 2>/dev/null
docker volume rm $(docker volume ls -q) 2>/dev/null
docker network rm $(docker network ls | grep -v "bridge\|host\|none" | awk '{print $1}') 2>/dev/null
docker builder prune -af
docker system prune -af --volumes

 rm -rf /tmp/*joiner*_tmp*
 rm -rf /tmp/*aggregator*_tmp*
 rm -rf /tmp/*tmp*
 rm -rf ./data/*
 rm -rf ./storage/*
 rm -rf ./tmp/*