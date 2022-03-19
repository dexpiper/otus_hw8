## Usage

Run:

cd to directory with memc_load.py

`$ python3 -m memx_load [-t --test] [-l --log] [--maxworkers] [--dry] [--pattern] [--idfa] [--gaid] [--adid] [--dvid]`


Dryrun example:

`$ python3 -m memx_load --log=log1.log --dry`

`[2022.03.19 14:49:12] I Memc loader started with options: {'test': False, 'log': 'log1.log', 'maxworkers': 3, 'dry': True, 'pattern': '/home/alex/Downloads/tarz/*.tsv.gz', 'idfa': '127.0.0.1:33013', 'gaid': '127.0.0.1:33014', 'adid': '127.0.0.1:33015', 'dvid': '127.0.0.1:33016'}`
`[2022.03.19 14:49:12] I Processing /home/alex/Downloads/tarz/20170929000000.tsv.gz`
`[2022.03.19 14:49:12] D 127.0.0.1:33013 - idfa:e7e1a50c0ec2747ca56cd9e1558c0d7c -> apps: 7942 apps: 8519 apps: 4232 apps: 3032 apps: 4766 apps: 9283 apps: 5682 apps: 155 apps: 5779 apps: 2260 apps: 3624 apps: 1358 apps: 2432 apps: 1212 apps: 528 apps: 8182 apps: 9061 apps: 9628 apps: 2055 apps: 4821 apps: 3550 apps: 4964 apps: 6924 apps: 6737 apps: 3784 apps: 5428 apps: 6980 apps: 8137 apps: 2129 apps: 8751 apps: 3000 apps: 5495 apps: 5674 apps: 3023 apps: 818 apps: 2864 apps: 8250 apps: 768 apps: 6931 apps: 3493 apps: 3749 apps: 8053 apps: 8815 apps: 8448 apps: 8757 apps: 272 apps: 5951 apps: 2831 apps: 7186 apps: 157 apps: 1629 apps: 2021 apps: 3338 apps: 9020 apps: 6679 apps: 8679 apps: 1477 apps: 7488 apps: 3751 apps: 7399 apps: 8556 apps: 5500 apps: 5333 apps: 3873 apps: 7070 apps: 3018 apps: 2734 apps: 4273 apps: 3723 apps: 4528 apps: 4657 apps: 4014 lat: 67.7835424444 lon: -22.8044005471 `
`[2022.03.19 14:49:12] D 127.0.0.1:33013 - idfa:f5ae5fe6122bb20d08ff2c2ec43fb4c4 -> apps: 4877 apps: 7862 apps: 7181 apps: 6071 apps: 2107 apps: 2826 apps: 2293 apps: 3103 apps: 9433 apps: 2794 apps: 4303 apps: 7500 apps: 5637 apps: 8935 apps: 6772 apps: 2481 apps: 1614 apps: 3946 apps: 7013 apps: 690 apps: 9474 apps: 1655 apps: 9718 apps: 4862 apps: 3367 apps: 3869 apps: 4255 apps: 9431 apps: 7333 apps: 5471 apps: 3267 apps: 7439 apps: 7202 apps: 7310 apps: 7875 apps: 1468 apps: 8146 apps: 9617 apps: 4336 apps: 8747 apps: 7815 lat: -104.68583244 lon: -51.24448376`

Memcache working example:

`$ memcache -l 127.0.0.1:33013, 127.0.0.1:33014, 127.0.0.1:33015, 127.0.0.1:33016`
`$ python3 -m memx_load --log=log2.log`

`[2022.03.19 14:57:05] I Memc loader started with options: {'test': False, 'log': 'log2.log', 'maxworkers': 3, 'dry': False, 'pattern': '/home/alex/Downloads/tarz/*.tsv.gz', 'idfa': '127.0.0.1:33013', 'gaid': '127.0.0.1:33014', 'adid': '127.0.0.1:33015', 'dvid': '127.0.0.1:33016'}`
`[2022.03.19 14:57:05] I Processing /home/alex/Downloads/tarz/20170929000000.tsv.gz`