ARG tag="11"
ARG workdir="/src"
ARG user="vscode"

FROM openjdk:${tag}

ARG workdir
ARG user

ENV DEBIAN_FRONTEND=noninteractive
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

WORKDIR /tmp

# Install common packages

RUN apt-get update --allow-releaseinfo-change && \
    apt-get -y install --no-install-recommends zsh curl git sudo vim ca-certificates

# Install Taskfile

RUN sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin ${task_version}

# Set the root password

RUN echo "root:root" | chpasswd

# Add the non-root user

RUN adduser ${user} --shell /bin/zsh
RUN adduser ${user} sudo
RUN echo "${user}:${user}" | chpasswd

# Install Oh My Zsh for the user

RUN su ${user} -c "$(curl -fsSL https://raw.github.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"

# Install maven

RUN apt-get install maven

WORKDIR ${workdir}

USER ${user}
