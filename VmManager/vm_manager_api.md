## start Vm
### request
```
{
    'method': 'allocate_vm',
    'timestamp': 'time.time()'
}
```
### response
```
{
    'request': 'entire request',
    'result': {
        'status': 'success',
        'message': 'vm started successfully'
    }
    'result': {
        'status': 'failed',
        'message': 'vm failed to start'
    }
}
```

## kill Vm
### request
```
{
    'method': 'remove_vm',
    'args': {
        'vm_id': vm_id
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
        'message': 'vm kill successfully'
    }
    'result': {
        'status': 'failed',
        'message': 'vm failed to be killed'
    }
}
```

## reset Vm
### request
```
{
    'method': 'reset_vm',
    'args': {
        'vm_id': vm_id
    },
    'timestamp': 'time.time()'}
```
### response
```
{
    'request': 'entire request',
    'result': {
        'status': 'success',
        'message': 'vm reset successfully'
    }
    'result': {
        'status': 'failed',
        'message': 'vm failed to be reset'
    }
}
```

## health
### request
```
{
    'method': 'get_health',
    'args': {
        'vm_id': 'vm_id',
    }
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

## Get vms
### request
```
{
    'method': 'get_vms',
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
    }
}
```