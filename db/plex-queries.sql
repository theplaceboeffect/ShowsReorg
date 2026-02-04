
with f as (
    select replace(replace(F.parent_path, '/mnt/nas5/media/videos/', ''), '/mnt/nas5/p2p/', '') || '/' || filename filepath
    from files F
    where F.removed_date is  null
    and F.filename like '%Ascension%'
)
, s as (
    select replace(replace(S.file_path, '/mnt/media/videos/', ''), '/mnt/p2p/','') filepath 
    from sonarr_files S
    where S.removed_date is  null
    and S.file_path like '%Ascension%'
)
, p as (
    select replace(replace(P.parent_path, '/mnt/nas5/media/videos/', ''), '/mnt/nas5/p2p/','') || '/' || P.filename filepath
    from plex_files P
    where P.removed_date is null
    and P.parent_path like '%Ascension%'
)
, combined as (
    select f.filepath filepath, s.filepath sonarr_filepath, p.filepath plex_filepath
    from f
    full outer join s on f.filepath = s.filepath
    full outer join p on f.filepath = p.filepath
)
, file_matches as (
    select filepath, sonarr_filepath, plex_filepath
    from combined C
    where filepath is not null and sonarr_filepath is not null and plex_filepath is not null
)
, file_mismatches as (
    select filepath, sonarr_filepath, plex_filepath
    from combined C
    where filepath is  null or sonarr_filepath is null or plex_filepath is null
)
select *
from file_mismatches M
--where M.filepath like '%/Ascension/%' or M.sonarr_filepath like '%/Ascension/%' or M.plex_filepath like '%Ascension%'
order by M.filepath
;