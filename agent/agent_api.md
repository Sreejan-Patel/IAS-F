## start process
### request
```
{
    'node_id': node_id,
    'method': 'start_process',
    'args': {
        config: {
            'name': 'process_name',
            'path': 'path_to_process',
            'command': 'command_to_run',
            'env': {
                'env1': 'value1',
                'env2': 'value2',
            }
        },
    'timestamp': 'time.time()'    }
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
    'result': {
        'status': 'failed',
        'message': 'process failed to start'
    }
}
```

## kill process
### request
```
{
    'node_id': node_id,
    'method': 'kill_process',
    'args': {
        'process_id': process_id
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
        'message': 'process kill successfully'
    }
    'result': {
        'status': 'failed',
        'message': 'process failed to be killed'
    }
}
```

## reset process
### request
```
{
    'node_id': node_id,
    'method': 'reset_process',
    'args': {
        'process_id': process_id
    },
    'timestamp': 'time.time()'}
```
### response
```
{
    'request': 'entire request',
    'result': {
        'status': 'success',
        'message': 'process reset successfully'
    }
    'result': {
        'status': 'failed',
        'message': 'process failed to be reset'
    }
}
```

## health
### request
```
{
    'node_id': node_id,
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

## processes
### request
```
{
    'node_id': node_id,
    'method': 'get_processes',
    'timestamp': 'time.time()'}
```
### response
```
{
    'request': 'entire request',
    'result': {
        'status': 'success',
        'process': [],
    }
    'result': {
        'status': 'failed',
        'message': 'unable to get health'
    }
}
```