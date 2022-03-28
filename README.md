# Usage

## Run:

cd to directory with memc_load.py

`$ python3 -m memc_load [-t --test] [-l --log] [--maxworkers] [--loginfo] [--dry] [--pattern] [--idfa] [--gaid] [--adid] [--dvid]`

* --test: run tests
* --log: store logs in <i>filename</i>
* --maxworkers: maximum workers
* --loginfo: if set, loglevel would be forced to INFO (even in --dry)
* --dry: dryrun without writing into memchached, logs would go to file or stdout
* --pattern: dir and name pattern to find .gz files to process
* --idfa, --gaid, --adid, --dvid: server addresses to store device memc's



## Example run

* with one file <i>20170929000000.tsv.gz</i>

head:

`[2022.03.20 22:19:26] I Memc loader started with options: {'test': False, 'log': 'log1.log', 'maxworkers': 5, 'dry': True, 'pattern': '/home/alex/Downloads/tarz/*.tsv.gz', 'idfa': '127.0.0.1:33013', 'gaid': '127.0.0.1:33014', 'adid': '127.0.0.1:33015', 'dvid': '127.0.0.1:33016'}`
`[2022.03.20 22:19:26] I Processing /home/alex/Downloads/tarz/20170929000000.tsv.gz`
`[2022.03.20 22:19:26] D 127.0.0.1:33013 - idfa:e7e1a50c0ec2747ca56cd9e1558c0d7c -> apps: 7942 apps: 8519 apps: 4232 apps: 3032 apps: 4766 apps: 9283 apps: 5682 apps: 155 apps: 5779 apps: 2260 apps: 3624 apps: 1358 apps: 2432 apps: 1212 apps: 528 apps: 8182 apps: 9061 apps: 9628 apps: 2055 apps: 4821 apps: 3550 apps: 4964 apps: 6924 apps: 6737 apps: 3784 apps: 5428 apps: 6980 apps: 8137 apps: 2129 apps: 8751 apps: 3000 apps: 5495 apps: 5674 apps: 3023 apps: 818 apps: 2864 apps: 8250 apps: 768 apps: 6931 apps: 3493 apps: 3749 apps: 8053 apps: 8815 apps: 8448 apps: 8757 apps: 272 apps: 5951 apps: 2831 apps: 7186 apps: 157 apps: 1629 apps: 2021 apps: 3338 apps: 9020 apps: 6679 apps: 8679 apps: 1477 apps: 7488 apps: 3751 apps: 7399 apps: 8556 apps: 5500 apps: 5333 apps: 3873 apps: 7070 apps: 3018 apps: 2734 apps: 4273 apps: 3723 apps: 4528 apps: 4657 apps: 4014 lat: 67.7835424444 lon: -22.8044005471 `
`[2022.03.20 22:19:26] D 127.0.0.1:33013 - idfa:f5ae5fe6122bb20d08ff2c2ec43fb4c4 -> apps: 4877 apps: 7862 apps: 7181 apps: 6071 apps: 2107 apps: 2826 apps: 2293 apps: 3103 apps: 9433 apps: 2794 apps: 4303 apps: 7500 apps: 5637 apps: 8935 apps: 6772 apps: 2481 apps: 1614 apps: 3946 apps: 7013 apps: 690 apps: 9474 apps: 1655 apps: 9718 apps: 4862 apps: 3367 apps: 3869 apps: 4255 apps: 9431 apps: 7333 apps: 5471 apps: 3267 apps: 7439 apps: 7202 apps: 7310 apps: 7875 apps: 1468 apps: 8146 apps: 9617 apps: 4336 apps: 8747 apps: 7815 lat: -104.68583244 lon: -51.24448376`

...

tail:

`[2022.03.20 22:39:12] D 127.0.0.1:33016 - dvid:bd9a5b7e1516d62b813d9f4ce6dbf6ee -> apps: 7710 apps: 5450 apps: 6857 apps: 7460 apps: 1177 apps: 3196 apps: 8734 apps: 1303 apps: 2542 apps: 5132 apps: 8621 apps: 7646 apps: 34 apps: 7879 apps: 5236 apps: 5031 apps: 8161 apps: 3816 apps: 3627 apps: 9300 apps: 1704 apps: 1321 apps: 987 apps: 6547 apps: 9787 apps: 8964 apps: 8816 apps: 1909 apps: 6986 apps: 9963 apps: 166 apps: 1849 apps: 4791 apps: 8005 apps: 851 apps: 5687 apps: 26 lat: -47.8291071322 lon: 73.0047398598 `
`[2022.03.20 22:39:12] D 127.0.0.1:33015 - adid:c0c402f15b5ae9ae94884143d79e7845 -> apps: 9098 apps: 4677 apps: 6924 apps: 8995 apps: 4671 apps: 2094 apps: 2610 apps: 2776 apps: 912 apps: 1349 apps: 8222 apps: 4253 apps: 8337 apps: 938 apps: 9849 apps: 1059 apps: 7488 apps: 367 apps: 9214 apps: 3795 apps: 4492 apps: 7535 apps: 7054 apps: 6023 apps: 3428 apps: 1758 apps: 4783 apps: 1538 apps: 2034 apps: 8483 apps: 4706 apps: 9267 apps: 4268 apps: 3790 apps: 211 apps: 8522 apps: 1614 apps: 2532 apps: 600 apps: 9464 apps: 9351 apps: 1518 apps: 8905 apps: 182 apps: 6939 apps: 7207 apps: 4457 apps: 7945 apps: 1320 apps: 8516 apps: 747 apps: 2043 apps: 3625 apps: 3639 apps: 1039 apps: 3026 apps: 5384 apps: 1785 apps: 5039 apps: 4322 apps: 7260 apps: 1928 apps: 9704 apps: 5615 apps: 6106 apps: 633 apps: 9501 apps: 5478 apps: 3676 apps: 1668 apps: 1429 apps: 1989 apps: 1756 apps: 9268 apps: 1068 apps: 3688 apps: 9642 apps: 7455 apps: 6800 apps: 6169 apps: 6890 apps: 5375 apps: 8743 apps: 3439 apps: 9878 apps: 7333 apps: 3282 apps: 929 apps: 5256 apps: 8557 apps: 5080 apps: 7256 lat: -21.2056085252 lon: -17.3329838649 `
`[2022.03.20 22:39:13] I Errors: 0, Processed: 3422995`
`[2022.03.20 22:39:13] I Acceptable error rate (0.0). Successfull load`

(Elapsed Time: 0:19:45)
