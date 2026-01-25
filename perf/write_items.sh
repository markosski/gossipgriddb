#!/bin/bash

# Configuration
URL="http://127.0.0.1:3001/items"
COUNT=1000

echo "Writing $COUNT items to $URL..."

for ((i=0; i<$COUNT; i++))
do
    # Construct JSON payload
    # Using printf to ensure we handle any potential escaping if needed, 
    # though for simple integers it's straightforward.
    JSON_DATA=$(printf '{"partition_key": "%d", "message": "item %d"}' $i $i)
    
    # Send POST request
    curl -s -X POST "$URL" \
         -H "Content-Type: application/json" \
         -d "$JSON_DATA" > /dev/null
    
    # Optional: progress indicator every 100 items
    if (( i % 100 == 0 )); then
        echo "Processed $i items..."
        sleep 0.1
    fi
done

echo "Done! Finished writing $COUNT items."
