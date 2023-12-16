#   Toy chunked log

This is a very basic implementation of a chunked log.
It creates a new chunk once the old one reaches the size limit.
Once the chunk count limit is reached, old chunks expire.

A `ChunkedLogReader` can seek within the chunks and change
chunks automatically providing a regular ReadSeeker API.

That's all the lib does.
