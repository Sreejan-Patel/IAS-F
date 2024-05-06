## create Node
### request
```
{
    'method': 'create_node',
    'timestamp': 'time.time()'
}
```
### response
```
{
    'request': 'entire request',
    'result': {
        'status': 'success',
        'message': 'node started successfully'
    }
    'result': {
        'status': 'failed',
        'message': 'node failed to start'
    }
}
```

## remove node
### request
```
{
    'method': 'remove_node',
    'args': {
        'node_id': 'node_id'
    },
    'timestamp': 'time.time()'
}
```
### response
```
{
    'request': 'entire request',
    'result': {
        'status': 'success',
        'message': 'Node kill successfully'
    }
    'result': {
        'status': 'failed',
        'message': 'Node failed to be killed'
    }
}
```

## reset node
### request
```
{
    'method': 'reset_node',
    'args': {
        'node_id': node_id
    },
    'timestamp': 'time.time()'}
```
### response
```
{
    'request': 'entire request',
    'result': {
        'status': 'success',
        'message': 'node reset successfully'
    }
    'result': {
        'status': 'failed',
        'message': 'node failed to be reset'
    }
}
```

## health
### request
```
{
    'method': 'get_health',
    'timestamp': 'time.time()'
}
```
### response
```
{
    'request': 'entire request',
    'result': {
        'status': 'success',
        'health': 'good', 'bad',
    }
    'result': {
        'status': 'failed',
        'message': 'unable to get health'
    }
}
```

## run_process_on_node
### request
```
{
    'method': 'run_process_on_node',
    'args': {
        'node_id': 'node_id',
        'config': {
            'name': 'process_name',
            'path': 'path_to_process',
            'command': 'command_to_run',
            'env': {
                'env1': 'value1',
                'env2': 'value2',
            }
        },
    }
    timestamp: 'time.time()'
}
```
### response
```
{
    'request': 'entire request',
    'result': {
        'status': 'success',
        'message': 'process started successfully'
    }
}
```