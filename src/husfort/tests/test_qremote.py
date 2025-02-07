if __name__ == "__main__":
    import argparse
    from husfort.qremote import CHost, scp_from_remote, scp_to_remote

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--host", required=True, type=str, help="host name")
    arg_parser.add_argument("--usr", required=True, type=str, help="user name")
    arg_parser.add_argument("--port", required=True, type=int, help="port")
    arg_parser.add_argument("--remote", required=True, type=str, help="remote path")
    arg_parser.add_argument("--local", required=True, type=str, help="local path")
    arg_parser.add_argument("-r", "--recursive", action="store_true", default=False, help="local path")
    arg_parser.add_argument("-u", "--upload", action="store_true", default=False,
                            help="use this to upload, else download")
    args = arg_parser.parse_args()
    host = CHost(hostname=args.host, username=args.usr, port=args.port)
    if args.upload:
        scp_to_remote(host, local_path=args.local, remote_path=args.remote)
    else:
        scp_from_remote(host, remote_path=args.remote, local_path=args.local)
