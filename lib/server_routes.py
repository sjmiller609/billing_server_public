@server.route('/info/activation_key/<id_input>')
def send_activation_key(id_input):
    if not str(request.remote_addr) == id_input:
        print("WARNING! an unauthorized IP is messing with us "+str(request.remote_addr))
        print("asking for dir: "+id_input)
        return None
    print("accepting request, serving activation_key to "+id_input)
    return send_from_directory("info/activation_key",id_input)

@server.route('/info/billing_info/<id_input>')
def send_billing_info(id_input):
    if not str(request.remote_addr) == id_input:
        print("WARNING! an unauthorized IP is messing with us "+str(request.remote_addr))
        print("asking for dir: "+id_input)
        return None
    print("accepting request, serving billing info to "+id_input)
    return send_from_directory("info/billing_info",id_input)

@server.route('/info/proxy_base/value',methods=["GET"])
def send_proxy_base():
    print("got request for proxy_base, sending")
    return send_from_directory("info/proxy_base","value")

@server.route('/info/task_count/value',methods=["GET"])
def send_task_count():
    print("got request for task_count, sending")
    return send_from_directory("info/task_count","value")

@server.route('/info/links/<place>',methods=["GET"])
def send_links(place):
    print("got request for link: "+place)
    return send_from_directory("info/links",place)

@server.route('/report/<id_input>',methods=['POST'])
def print_report(id_input):
    print("----------")
    if not str(request.remote_addr) == id_input:
        print("WARNING! an unauthorized IP is messing with us "+str(request.remote_addr))
        return None
    print(id_input+" reports: "+request.form["data"])
    print("----------")
    return redirect("www.google.com")

