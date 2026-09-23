with source as (
    select
        sessionId,
        ts
    from {{source('raw', 'session_timestamp') }}
    WHERE sessionId IS NOT NULL
    
)

select * from source
