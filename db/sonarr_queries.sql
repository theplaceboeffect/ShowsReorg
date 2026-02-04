select * from sonarr_files;

select * from files;
-----
    select *, replace(replace(F.parent_path, '/mnt/nas5/media/videos/', ''), '/mnt/nas5/p2p/', '') || '/' || filename filepath
    from files F
    where F.filename like '%Citizen.Khan%'
    ;
-----
    select replace(replace(S.file_path, '/mnt/media/videos/', ''), '/mnt/p2p/','') sonarr_filepath 
    from sonarr_files S
    where S.removed_date is null
    and S.file_path like '%Amandaland%'
    ;
    
---
with f as (
    select replace(replace(F.parent_path, '/mnt/nas5/media/videos/', ''), '/mnt/nas5/p2p/', '') || '/' || filename filepath
    from files F
    where F.removed_date is  null
    --and F.filename like '%Cracker%'
)
, s as (
    select replace(replace(S.file_path, '/mnt/media/videos/', ''), '/mnt/p2p/','') filepath 
    from sonarr_files S
    where S.removed_date is  null
    --and S.file_path like '%Cracker%'
),
file_matches as (
    select f.filepath filepath, s.filepath sonarr_filepath
    from f
    full outer join s on f.filepath = s.filepath
    where f.filepath is not null and s.filepath is not null
),
file_mismatches as (
    select f.filepath filepath, s.filepath sonarr_filepath
    from f
    full outer join s on f.filepath = s.filepath
    where f.filepath is null or s.filepath is null
)
select *
from file_mismatches f
where filepath like '%Citizen.Khan%' or sonarr_filepath like '%Citizen.Khan%'
order by f.filepath
;

select * from files where parent_path like '%Khan%'
