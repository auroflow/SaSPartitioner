nodes="node11 node23 node193"

# Check if user provided nodes
if [ -z "$1" ]; then
    echo "No nodes provided. Using default nodes: $nodes"
else
    nodes=$1
fi

# Push code to nodes
for node in $nodes; do
    echo "Pushing code to $node"
    rsync -avzh --exclude "*.pyc" --exclude "nohup.out" --exclude ".git/" --exclude ".idea/" --exclude "__pycache__/" . $node:/home/user/code/saspartitioner/scripts/rl
done
