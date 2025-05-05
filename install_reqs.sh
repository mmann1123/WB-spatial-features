#!/bin/bash
set -e

# Create and activate environment
mamba remove -n spfeas --all -y
mamba create -n spfeas python=3.6 -y
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate spfeas


# Install dependencies
mamba install -c conda-forge pytables qtconsole libstdcxx-ng=12 opencv gdal=2  -y

# Create requirements file
cat << EOF > spfeas_requirements.txt
astroid==1.4.9
astropy
backports.shutil-get-terminal-size==1.0.0
bitarray==0.8.1
blaze==0.10.1
bokeh==0.12.5
boto==2.46.1
bottleneck==1.2.1
cerberus==1.1
certifi==2018.4.16
chardet==3.0.3
colorama==0.3.9
contextlib2==0.5.5
cython==0.25.2
cytoolz==0.8.2
distributed==1.16.3
fastcache==1.0.2
geojson==2.3.0
gevent==1.2.1
h5py==2.7.0
humanize==0.5.1
isort==4.2.5
jedi==0.10.2
jsonschema==2.6.0
lxml==3.7.3
mapbox-vector-tile==1.2.0
mbutil==0.3.0
mpmath==0.19
nbconvert==5.1.1
nltk==3.2.3
nose==1.3.7
numpy
numba==0.33.0
numpydoc==0.6.0
opencv-python==4.5.5.64
openpyxl==2.4.7
partd==0.3.8
pathlib2==2.2.1
patsy==0.4.1
pep8==1.7.0
pexpect==4.2.1
pickleshare==0.7.4
ply==3.10
prompt-toolkit==1.0.14
ptyprocess==0.5.1
pycrypto==2.6.1
pycurl==7.43.0
pyflakes==1.5.0
pylint==1.6.4
pyodbc==4.0.16
pyopenssl==17.0.0
pytest==3.0.7
pyzmq==16.0.2
qtawesome==0.4.4
qtconsole==4.3.0
rasterio
rope-py3k==0.9.4.post1
scikit-learn==0.22
seaborn==0.7.1
shapely==1.6.3
simplegeneric==0.8.1
singledispatch==3.4.0.3
sklearn==0.0
sphinx==1.5.6
sympy==1.0
tables==3.4.4
terminado==0.6
unicodecsv==0.14.1
widgetsnbextension==2.0.0
xlrd==1.0.0
xlsxwriter==0.9.6
xlwt==1.2.0
EOF

# Install Python packages
pip install -r spfeas_requirements.txt

# Download and install mpglue
wget https://github.com/jgrss/mpglue/archive/refs/tags/0.2.14.tar.gz -O mpglue-0.2.14.tar.gz
pip install mpglue-0.2.14.tar.gz

# Clone and install spfeas
if [ ! -d "spfeas" ]; then
    git clone https://github.com/jgrss/spfeas.git
fi
cd spfeas
pip install .
