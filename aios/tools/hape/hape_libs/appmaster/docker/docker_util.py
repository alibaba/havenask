import time

from hape_libs.utils.shell import SSHShell
from hape_libs.utils.logger import Logger
        
class DockerContainerUtil():
    @staticmethod
    def check_container(ip, name):
        shell = SSHShell(ip)
        check_command = "docker ps --format {{{{.Names}}}} | grep ^{}$".format(name)
        out = shell.execute_command(check_command)
        if len(out.strip()) != 0:
            Logger.info("Container {} is started in {}".format(name, ip))
            return True
        else:
            Logger.info("Container {} is not exists in {}".format(name, ip))
            return False
        
    @staticmethod
    def create_container(ip, name, workdir, homedir, user, cpu, mem, image):
        Logger.info("Begin to create container {} in {}".format(name, ip))
        if DockerContainerUtil.check_container(ip, name):
            Logger.warning("Container {} already exists in {}, will stop it".format(name, ip))
            DockerContainerUtil.stop_container(ip, name)
            
        shell = SSHShell(ip)
        
        check_command = "ls {}".format(workdir)
        _, notexists = shell.execute_command(check_command, grep_text="No such")
        if notexists:
            command = "mkdir -p  {}".format(workdir)
            shell.execute_command(command)
            _, notexists = shell.execute_command(check_command, grep_text="No such")
            if notexists:
                Logger.error("Failed to create directory {}".format(workdir))
                return False
            
        command = "docker run --workdir {} --volume=\"/etc/group:/home/.group:ro\" --volume=\"/etc/passwd:/home/.passwd:ro\"  --volume=\"/etc/hosts:/etc/hosts:ro\"\
 --ulimit nofile=655350:655350  --ulimit memlock=-1  --cpu-quota={} --cpu-period=100000 --memory={}m -v {}:{}  -d --network=host --privileged --name {} {} /sbin/init".format(
                workdir, int(cpu) * 1000, mem, homedir, homedir, name, image)

        shell.execute_command(command)
        
        workdir = workdir + "/" + name
        
        command = "docker exec -t {} bash -c \"cp /home/.passwd /etc/passwd && cp /home/.group /etc/group\"".format(name)
        shell.execute_command(command)

        if isinstance(user, tuple):
            user = user[0]
        command = "docker exec -t --user {} {} bash -c \"mkdir -p {}\"".format(user, name, workdir)
        shell.execute_command(command)
        
        if not DockerContainerUtil.check_container(ip, name):
            Logger.error("Failed to create container {} in ip:{}".format(name, ip))
            return False
        else:
            Logger.info("Succeed to create container {} in ip:{}".format(name, ip))
            return True
    
    @staticmethod
    def check_process_pid(ip, name, command):
        shell = SSHShell(ip)
        check_command = "docker exec -t {} bash -c \"ps -ef | grep {} | grep -v grep\"".format(name, command)
        out = shell.execute_command(check_command)
        for line in out.strip().split("\n"):
            splits = line.split()
            if len(splits) > 1 and splits[1].isdigit():
                Logger.info("Found process in ip {}, pid {}".format(ip, splits[1]))
                return splits[1]
            
        return None
     
    @staticmethod   
    def start_process(ip, name, user, workdir, envs, args, command):
        Logger.info("Begin to start process {} in {}".format(command, ip))
        
        full_process_command = ""
        if len(envs)!=0:
            full_process_command += " ".join(envs) + " "
        full_process_command += command
        if len(args)!=0:
            full_process_command += " " + " ".join(args)
        
        full_process_command += " 1> stdout.log 2>stderr.log"
            
        shell = SSHShell(ip)
        docker_command = "docker exec --user {} -t {} bash -c \"cd {} && echo \\\"{}\\\" > process_starter.sh\"".format(user, name, workdir, full_process_command)
        shell.execute_command(docker_command)
        
        sshme = "docker exec -it --user {} {} bash".format(user, name)
        docker_command = "docker exec --user {} -t {} bash -c \"cd {} && echo \\\"{}\\\" > sshme\"".format(user, name, workdir, sshme)
        shell.execute_command(docker_command)
        
        docker_command = "docker exec --user {}  -dt {} bash -c \"cd {} && sh process_starter.sh\"".format(user, name, workdir)
        shell.execute_command(docker_command)
        count = 10
        Logger.info("Wait util process is started")
        while count:
            if DockerContainerUtil.check_process_pid(ip, name, command) == None:
                Logger.warning("Processor not found, wait {} times".format(count))
                count -= 1
                time.sleep(5)
            else:
                Logger.info("Succeed to start process {}".format(command))
                return True
        Logger.error("Failed to start process {}".format(command))
        return False
    
    @staticmethod
    def execute_command(ip, name, workdir, user, envs, args, command, grep_text = None):
        full_process_command = ""
        if len(envs)!=0:
            full_process_command += " ".join(envs) + " "
        full_process_command += command
        if len(args)!=0:
            full_process_command += " " + " ".join(args)
        
        shell = SSHShell(ip)
        workdir = workdir + "/" + name
        command = "docker exec --user {} -t {} bash -c \"cd {} &&{}\"".format(user, name, workdir, full_process_command)
        return shell.execute_command(command, grep_text=grep_text)
    
    @staticmethod
    def stop_container(ip, name):
        Logger.info("Stop container {} in {}".format(name, ip))
        shell = SSHShell(ip)
        shell.execute_command("docker ps --all --format {{{{.Names}}}} | grep ^{}$ | xargs docker rm -f".format(name))
