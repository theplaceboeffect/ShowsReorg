select *
from scan_dirs
;

select *
from scans
;

select count(1)
from files
;

select filename, count(1)
from files
where removed_date is null
group by filename
having count(1) > 1
order by 2 desc
;

-- - select * from files where removed_date is not null;
select filename, parent_path, creation_date, added_date, removed_date
from files
where
    removed_date is null
   and (
      lower(filename) LIKE '%.mp4'
   OR lower(filename) LIKE '%.mkv'
   OR lower(filename) LIKE '%.avi'
   OR lower(filename) LIKE '%.mov'
   OR lower(filename) LIKE '%.wmv'
   OR lower(filename) LIKE '%.flv'
   OR lower(filename) LIKE '%.webm'
   OR lower(filename) LIKE '%.mpeg'
   OR lower(filename) LIKE '%.mpg'
   OR lower(filename) LIKE '%.m4v'
   OR lower(filename) LIKE '%.ts'
   )
;
