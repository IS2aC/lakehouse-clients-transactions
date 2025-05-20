FROM quay.io/astronomer/astro-runtime:12.9.0

# Basculer en root pour les installations
USER root

RUN apt-get update && apt-get install -y openssh-client

# Installer les dépendances nécessaires à SDKMAN et Java
RUN apt-get update && apt-get install -y curl unzip zip \
 && rm -rf /var/lib/apt/lists/*



# Changer d’utilisateur pour astro (permet d’installer SDKMAN dans son $HOME)
USER astro

# Installer SDKMAN et Java 11 pour l'utilisateur astro
ENV SDKMAN_DIR="/home/astro/.sdkman"
RUN curl -s "https://get.sdkman.io" | bash \
 && bash -c "source $SDKMAN_DIR/bin/sdkman-init.sh && sdk install java 11.0.21-tem"

# Configurer JAVA_HOME et PATH pour Java 11
ENV JAVA_HOME="/home/astro/.sdkman/candidates/java/current"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# SDKMAN a besoin d’un init dans les shells non interactifs
RUN echo "source /home/astro/.sdkman/bin/sdkman-init.sh" >> /home/astro/.bashrc
