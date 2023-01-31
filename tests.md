### Consistency Tests

| Test No. | Test Name | Description |
| :--- | :--- | :--- |
| 1 | PutGetCheck | Upload a 4K object (version) on a node and verify GET on object succeeds across all nodes|
| 2 | PutGetCheck2 | Upload a multipart object - one part per node, and after finalizing the upload verify GET succeeds on all nodes|
| 3 | PutHeadCheck | Upload a 4K object (version) on a node and verify all metadata with HEAD across the nodes|
| 4 | PutHeadCheck2 | Upload a multipart object - one part per node, and after finalizing the upload verify and verify all metadata with HEAD across the nodes|
| 5 | PutListCheck | Upload two 4K objects on a node and verify if both versions get listed on each of the nodes|
| 6 | PutListCheck2 | Upload a multipart object - one part per node, and after finalizing the upload perform LIST on all nodes to verify number of versions listed|