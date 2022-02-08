# smart_duplicate_file_finder

**WORK IN PROGRESS**

This is an attempt at creating a "smart/clever" duplicate file finder in python.

**WARNING: at this stage it is probably confusing for other people to understand the code, partly because it's very preliminary and it relies on files I have not pushed - and it just needs a lot of cleaning/refactoring, before this is useful to other people.**



The motivation for this repository is: Over the years, I've collected a massive amount of data which I've backed up. With cheaper and cheaper harddrives, unfortunately for many years it has haunted me that the structure of my backups evolved into a mess. So there are many duplicate files and folders in different subdirectories. Tools already exists that can find duplicate files. But the strategy of the tools I found was to either match filename, optionally incl filesize (optionally date/time) or make a cryptographic hash, for determining if files are duplicates. The cryptographic hash is a really good and reliable method - and it works fine for files up to some giga-bytes. After that, things become *tremendously* slow (think DVD images, .ISO-images, etc)...

Example: MD5SUM for a 2 GB file on SSD takes around 5 seconds which I think is acceptable - *BUT* on a mechanical harddrive, it takes around 30 seconds for me... My backups are not on SSD, but on mechanical harddrives. In this project, I've decided to read files into chunks of 1 MB and I'm willing to wait around 15 seconds  per file - so I'll probably need 1000 chunks of data. The next problem to solve is that I want to calculate the hash on a representative random part of apparantly identical files (=same file-sizes). I take 1000 random numbers and distribute them evenly over the filesize and this provides me with the chunks (chunk numbers) I'll use for calculating my cryptographic hash. Once completed, I'm 99% confident that the files are identical. This is where a human immediately can see/recognize what needs to be done and I've solved this much faster than any other (free) tool I found - and I don't want to pay for a tool to do this for me.

Sorry, but unfortunately, this code needs to be rewritten and improved and I haven't really got time at the moment - but I will put this up and maybe/possible fix the remaining later.
