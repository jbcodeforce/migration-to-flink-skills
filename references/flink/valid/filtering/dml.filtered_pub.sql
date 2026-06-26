insert into filtered_publications (book_id, author, title)
select book_id, author, title
from all_publications
where author = 'George R. R. Martin';