

echo "Creating conda environment using /application/dependencies/python/environment.yml"
sudo conda update conda -y && conda env create --file=/application/dependencies/python/environment.yml

echo "Installing conda-build"
sudo conda install conda-build -y

if [ -d "/application/python" ]
then
        echo "Building and installing conda packages in /application/python"
        for f in /application/python/* ; do
            if [ -d "$f" ]; then
	            if [[ -n $(find ${f} -type d -name "conda.recipe") ]]; then
                        package_name=$(awk '/name:/{print $NF}' ${f}/conda.recipe/meta.yaml)
                        echo "Installing ${package_name} to conda environment env_ewf_wfp_03_03_01"
                        sudo conda build ${f}
                        sudo conda install -n env_ewf_wfp_03_03_01 ${package_name} --use-local -y
            	    fi
            fi
        done
fi
