import zipfile
import os
from lxml import etree
import ast
import logging
import shutil
import glob
from geopy.distance import geodesic  # Added for distance calculation
import re  # For regular expressions
import pyodbc  # For Microsoft SQL Server connection

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')
logging.disable(logging.CRITICAL)  # This will suppress all logging calls

# Connection parameters for MS SQL Server
server_name = 'sql_server,1433'  # Replace with your server name
database_name = 'your_database_name'  # Replace with your database name
username = 'sa'  # Replace with your username
password = 'YourStrong!Password'  # Replace with your password

def init_db():
    """
    Initializes the Microsoft SQL Server database with necessary tables and columns.
    """
    conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};'
                          f'SERVER={server_name};'
                          f'DATABASE={database_name};'
                          f'UID={username};'
                          f'PWD={password};'
                          f'Encrypt=no;')
    cursor = conn.cursor()

    # Check if the database exists
    cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{database_name}'")
    result = cursor.fetchone()

    # If the database does not exist, create it
    if result is None:
        cursor.execute(f"CREATE DATABASE {database_name}")
        conn.commit()
        logging.info(f"Database {database_name} created successfully.")

    # Now connect to the actual database
    conn.close()  # Close the connection to the master database
    conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};'
                          f'SERVER={server_name};'
                          f'DATABASE={database_name};'
                          f'UID={username};'
                          f'PWD={password};'
                          f'Encrypt=no;')
    cursor = conn.cursor()

    # Create placemarks table if it does not exist
    create_placemarks_table_sql = '''
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'placemarks')
    BEGIN
        CREATE TABLE placemarks (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name NVARCHAR(MAX),
            description NVARCHAR(MAX),
            coordinates NVARCHAR(MAX),
            longitude FLOAT,
            latitude FLOAT,
            altitude FLOAT,
            heading FLOAT,
            tilt FLOAT,
            range FLOAT,
            altitude_mode NVARCHAR(MAX),
            line_color NVARCHAR(MAX),
            line_width INT,
            line_opacity FLOAT,
            poly_color NVARCHAR(MAX),
            poly_opacity FLOAT,
            icon_href NVARCHAR(MAX),
            icon_scale FLOAT,
            icon_color NVARCHAR(MAX),
            label_color NVARCHAR(MAX),
            label_scale NVARCHAR(MAX),
            extended_data NVARCHAR(MAX),
            folder_hierarchy NVARCHAR(MAX),
            attributes NVARCHAR(MAX),
            geometry_type NVARCHAR(MAX),  -- Column to store geometry type
            geometry_xml NVARCHAR(MAX),    -- Column to store geometry XML
            line_length FLOAT,     -- Column to store line length
            date_acq NVARCHAR(MAX),        -- New column for date
            voltage NVARCHAR(MAX),         -- New column for voltage
            cable NVARCHAR(MAX),           -- New column for cable
            from_str NVARCHAR(MAX),        -- New column for From Str.
            to_str NVARCHAR(MAX),          -- New column for To Str.
            disp_condition NVARCHAR(MAX),  -- New column for Disp. Condition
            five_digit_code NVARCHAR(MAX), -- New column for 5 Digit Code
            county NVARCHAR(MAX),          -- New column for County
            address NVARCHAR(MAX),         -- New column for Address
            station_voltage NVARCHAR(MAX), -- New column for Station Voltage
            gln_x NVARCHAR(MAX),           -- New column for GLN X
            gln_y NVARCHAR(MAX)            -- New column for GLN Y
        );
    END
    '''
    cursor.execute(create_placemarks_table_sql)
    conn.commit()

    # Check for the existence of new columns, add them if they don't exist
    cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'placemarks'")
    columns = [row[0] for row in cursor.fetchall()]
    new_columns = {
        'geometry_type': 'NVARCHAR(MAX)',
        'geometry_xml': 'NVARCHAR(MAX)',
        'line_length': 'FLOAT',
        'date_acq': 'NVARCHAR(MAX)',
        'voltage': 'NVARCHAR(MAX)',
        'cable': 'NVARCHAR(MAX)',
        'from_str': 'NVARCHAR(MAX)',
        'to_str': 'NVARCHAR(MAX)',
        'disp_condition': 'NVARCHAR(MAX)',
        'five_digit_code': 'NVARCHAR(MAX)',
        'county': 'NVARCHAR(MAX)',
        'address': 'NVARCHAR(MAX)',
        'station_voltage': 'NVARCHAR(MAX)',
        'gln_x': 'NVARCHAR(MAX)',
        'gln_y': 'NVARCHAR(MAX)'
    }
    for column_name, column_type in new_columns.items():
        if column_name not in columns:
            cursor.execute(f"ALTER TABLE placemarks ADD {column_name} {column_type};")
            conn.commit()

    # Create groundoverlays table if it does not exist
    create_groundoverlays_table_sql = '''
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'groundoverlays')
    BEGIN
        CREATE TABLE groundoverlays (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name NVARCHAR(MAX),
            visibility INT,
            color NVARCHAR(MAX),
            icon_href NVARCHAR(MAX),
            coordinates NVARCHAR(MAX),  -- Add coordinates field for LatLonQuad
            north FLOAT,
            south FLOAT,
            east FLOAT,
            west FLOAT,
            rotation FLOAT,
            view_bound_scale FLOAT,  -- New column added here
            folder_hierarchy NVARCHAR(MAX),
            attributes NVARCHAR(MAX),
            extended_data NVARCHAR(MAX)
        );
    END
    '''
    cursor.execute(create_groundoverlays_table_sql)
    conn.commit()

    # Check for new columns in groundoverlays
    cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'groundoverlays'")
    groundoverlay_columns = [row[0] for row in cursor.fetchall()]
    if 'view_bound_scale' not in groundoverlay_columns:
        cursor.execute("ALTER TABLE groundoverlays ADD view_bound_scale FLOAT;")
        conn.commit()
    if 'extended_data' not in groundoverlay_columns:
        cursor.execute("ALTER TABLE groundoverlays ADD extended_data NVARCHAR(MAX);")
        conn.commit()

    # Create networklinks table if it does not exist
    create_networklinks_table_sql = '''
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'networklinks')
    BEGIN
        CREATE TABLE networklinks (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name NVARCHAR(MAX),
            visibility INT,
            longitude FLOAT,
            latitude FLOAT,
            altitude FLOAT,
            heading FLOAT,
            tilt FLOAT,
            range FLOAT,
            altitude_mode NVARCHAR(MAX),
            href NVARCHAR(MAX),
            viewRefreshMode NVARCHAR(MAX),
            viewRefreshTime FLOAT,
            folder_hierarchy NVARCHAR(MAX),
            attributes NVARCHAR(MAX),
            extended_data NVARCHAR(MAX)
        );
    END
    '''
    cursor.execute(create_networklinks_table_sql)
    conn.commit()

    # Check for new columns in networklinks
    cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'networklinks'")
    networklink_columns = [row[0] for row in cursor.fetchall()]
    if 'extended_data' not in networklink_columns:
        cursor.execute("ALTER TABLE networklinks ADD extended_data NVARCHAR(MAX);")
        conn.commit()

    logging.info("Database initialized successfully.")
    return conn

def extract_data_from_description(description):
    """
    Parses the description text and extracts specific data fields.
    """
    data = {
        'date_acq': None,
        'voltage': None,
        'cable': None,  # This will store Conductor Type
        'from_str': None,
        'to_str': None,
        'disp_condition': None,
        'five_digit_code': None,
        'county': None,
        'address': None,
        'station_voltage': None,
        'gln_x': None,
        'gln_y': None
    }

    if description is None:
        return data

    # Remove HTML tags if present
    description_text = re.sub(r'<[^>]+>', '', description)

    # Split the description into lines
    lines = description_text.splitlines()

    # Check for HTML table format
    if '<td>' in description:
        # Extract data from HTML table format
        matches = re.findall(r'<td>(.*?)</td>\s*<td>(.*?)</td>', description, re.DOTALL)
        for key, value in matches:
            key = key.strip().lower()
            value = value.strip()
            if key in ['date_acq', 'date']:
                data['date_acq'] = value
            elif key in ['voltage', 'station voltage']:
                data['voltage'] = value
            elif key in ['cable', 'conductor type']:  # Added 'conductor type' mapping
                data['cable'] = value
            elif key == 'from str.':
                data['from_str'] = value
            elif key == 'to str.':
                data['to_str'] = value
            elif key == 'disp. condition':
                data['disp_condition'] = value
            elif key == '5 digit code':
                data['five_digit_code'] = value
            elif key == 'county':
                data['county'] = value
            elif key == 'address':
                data['address'] = value
            elif key == 'gln x':
                data['gln_x'] = value
            elif key == 'gln y':
                data['gln_y'] = value
            elif key == 'station voltage':
                data['station_voltage'] = value
    else:
        # Extract data from plain text format
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # Use regex to extract key-value pairs
            match = re.match(r'(.+?):\s*(.*)', line)
            if match:
                key, value = match.groups()
                key = key.strip().lower()
                value = value.strip()
                if key in ['date_acq', 'date']:
                    data['date_acq'] = value
                elif key in ['voltage', 'station voltage']:
                    data['voltage'] = value
                elif key in ['cable', 'conductor type']:  # Added 'conductor type' mapping
                    data['cable'] = value
                elif key == 'from str.':
                    data['from_str'] = value
                elif key == 'to str.':
                    data['to_str'] = value
                elif key == 'disp. condition':
                    data['disp_condition'] = value
                elif key == '5 digit code':
                    data['five_digit_code'] = value
                elif key == 'county':
                    data['county'] = value
                elif key == 'address':
                    data['address'] = value
                elif key == 'gln x':
                    data['gln_x'] = value
                elif key == 'gln y':
                    data['gln_y'] = value
                elif key == 'station voltage':
                    data['station_voltage'] = value
            else:
                # Handle cases where key and value are in the same line without colon
                # For example: "Date 4/6/2023"
                match = re.match(r'(date|date_acq)\s+(\S+)', line.lower())
                if match:
                    key, value = match.groups()
                    data['date_acq'] = value.strip()

    return data


def insert_placemark(conn, placemark_data):
    """
    Inserts a Placemark record into the database.
    """
    cursor = conn.cursor()
    cleaned_coordinates = placemark_data['coordinates'].strip() if placemark_data['coordinates'] is not None else None

    try:
        cursor.execute('''
            INSERT INTO placemarks (
                name, description, coordinates, longitude, latitude, altitude, heading, tilt, range, altitude_mode,
                line_color, line_width, line_opacity, poly_color, poly_opacity, icon_href, icon_scale, icon_color,
                label_color, label_scale, extended_data, folder_hierarchy, attributes, geometry_type, geometry_xml, line_length,
                date_acq, voltage, cable, from_str, to_str, disp_condition, five_digit_code, county, address, station_voltage, gln_x, gln_y
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            placemark_data.get('name'),
            placemark_data.get('description'),
            cleaned_coordinates,
            placemark_data.get('longitude'),
            placemark_data.get('latitude'),
            placemark_data.get('altitude'),
            placemark_data.get('heading'),
            placemark_data.get('tilt'),
            placemark_data.get('range'),
            placemark_data.get('altitude_mode'),
            placemark_data.get('line_color'),
            placemark_data.get('line_width'),
            placemark_data.get('line_opacity'),
            placemark_data.get('poly_color'),
            placemark_data.get('poly_opacity'),
            placemark_data.get('icon_href'),
            placemark_data.get('icon_scale'),
            placemark_data.get('icon_color'),
            placemark_data.get('label_color'),
            placemark_data.get('label_scale'),
            str(placemark_data.get('extended_data')),
            ' > '.join(placemark_data['folder_hierarchy']) if placemark_data.get('folder_hierarchy') else None,
            str(placemark_data.get('attributes')),
            placemark_data.get('geometry_type'),
            placemark_data.get('geometry_xml'),
            placemark_data.get('line_length'),
            placemark_data.get('date_acq'),
            placemark_data.get('voltage'),
            placemark_data.get('cable'),  # Conductor Type stored here
            placemark_data.get('from_str'),
            placemark_data.get('to_str'),
            placemark_data.get('disp_condition'),
            placemark_data.get('five_digit_code'),
            placemark_data.get('county'),
            placemark_data.get('address'),
            placemark_data.get('station_voltage'),
            placemark_data.get('gln_x'),
            placemark_data.get('gln_y')
        ))

        conn.commit()
        logging.debug(f"Inserted Placemark: {placemark_data.get('name')}")
    except Exception as e:
        logging.error(f"Failed to insert placemark '{placemark_data.get('name')}': {e}")

def insert_groundoverlay(conn, overlay_data):
    """
    Inserts a GroundOverlay record into the database.
    """
    cursor = conn.cursor()

    try:
        cursor.execute('''
            INSERT INTO groundoverlays (
                name, visibility, color, icon_href, coordinates, north, south, east, west, rotation, view_bound_scale, folder_hierarchy, attributes, extended_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            overlay_data['name'],
            overlay_data['visibility'],
            overlay_data['color'],
            overlay_data['icon_href'],
            overlay_data['coordinates'],  # Store LatLonQuad coordinates here if available
            overlay_data['north'],  # Store LatLonBox values
            overlay_data['south'],
            overlay_data['east'],
            overlay_data['west'],
            overlay_data['rotation'],  # New field inserted here
            overlay_data['view_bound_scale'],  # New field inserted here
            ' > '.join(overlay_data['folder_hierarchy']) if overlay_data['folder_hierarchy'] else None,
            str(overlay_data['attributes']),
            str(overlay_data['extended_data'])  # Store extended_data as string
        ))
        conn.commit()
        logging.debug(f"Inserted GroundOverlay: {overlay_data['name']}")
    except Exception as e:
        logging.error(f"Failed to insert GroundOverlay '{overlay_data['name']}': {e}")

def insert_networklink(conn, networklink_data):
    """
    Inserts a NetworkLink record into the database.
    """
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO networklinks (
                name, visibility, longitude, latitude, altitude, heading, tilt, range, altitude_mode,
                href, viewRefreshMode, viewRefreshTime, folder_hierarchy, attributes, extended_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            networklink_data['name'],
            networklink_data['visibility'],
            networklink_data['longitude'],
            networklink_data['latitude'],
            networklink_data['altitude'],
            networklink_data['heading'],
            networklink_data['tilt'],
            networklink_data['range'],
            networklink_data['altitude_mode'],
            networklink_data['href'],  # Correctly extract href from either Link or Url
            networklink_data['viewRefreshMode'],  # Correctly extract viewRefreshMode
            networklink_data['viewRefreshTime'],  # Correctly extract viewRefreshTime
            ' > '.join(networklink_data['folder_hierarchy']) if networklink_data['folder_hierarchy'] else None,
            str(networklink_data.get('attributes')),
            str(networklink_data.get('extended_data'))  # Optional
        ))
        conn.commit()
        logging.debug(f"Inserted NetworkLink: {networklink_data['name']}")
    except Exception as e:
        logging.error(f"Failed to insert NetworkLink '{networklink_data['name']}': {e}")


def extract_kml_from_kmz(kmz_file, extract_path):
    """
    Extracts the KML file from a KMZ archive.
    """
    if not os.path.exists(extract_path):
        os.makedirs(extract_path)

    with zipfile.ZipFile(kmz_file, 'r') as z:
        z.extractall(extract_path)

        for file in z.namelist():
            if file.endswith('doc.kml') or file.endswith('.kml'):
                return os.path.join(extract_path, file)
    return None


def fix_kml_namespace(kml_file):
    """
    Ensures that the KML file has the correct namespaces.
    """
    with open(kml_file, 'r', encoding='utf-8') as file:
        kml_content = file.read()

    # Check if the <kml> tag has the standard KML namespace
    if 'xmlns="http://www.opengis.net/kml/2.2"' not in kml_content:
        # Add the standard KML namespace
        kml_content = kml_content.replace(
            '<kml',
            '<kml xmlns="http://www.opengis.net/kml/2.2"'
        )

    # Check if the 'xmlns:xsi' is present, add it if missing
    if 'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' not in kml_content:
        kml_content = kml_content.replace(
            '<kml ',
            '<kml xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        )

    corrected_kml_file_path = os.path.join(os.path.dirname(kml_file), 'corrected_doc.kml')
    with open(corrected_kml_file_path, 'w', encoding='utf-8') as file:
        file.write(kml_content)

    logging.info(f"Fixed KML namespace and saved to: {corrected_kml_file_path}")
    return corrected_kml_file_path


def parse_styles_and_maps(root, ns):
    """
    Parses <Style> and <StyleMap> elements from the KML.
    """
    styles = {}
    style_maps = {}

    for style in root.findall('.//kml:Style', ns):
        style_id = style.get('id')
        if style_id:
            styles[style_id] = style

    for style_map in root.findall('.//kml:StyleMap', ns):
        style_map_id = style_map.get('id')
        if style_map_id:
            style_maps[style_map_id] = style_map

    return styles, style_maps



def extract_lookat(element, ns):
    """
    Extracts LookAt information from a Placemark or NetworkLink.
    """
    lookat = element.find('kml:LookAt', ns)
    if lookat is not None:
        longitude = lookat.find('kml:longitude', ns).text if lookat.find('kml:longitude', ns) is not None else None
        latitude = lookat.find('kml:latitude', ns).text if lookat.find('kml:latitude', ns) is not None else None
        altitude = lookat.find('kml:altitude', ns).text if lookat.find('kml:altitude', ns) is not None else None
        heading = lookat.find('kml:heading', ns).text if lookat.find('kml:heading', ns) is not None else None
        tilt = lookat.find('kml:tilt', ns).text if lookat.find('kml:tilt', ns) is not None else None
        range_val = lookat.find('kml:range', ns).text if lookat.find('kml:range', ns) is not None else None
        altitude_mode = lookat.find('gx:altitudeMode', ns).text if lookat.find('gx:altitudeMode', ns) is not None else None
    else:
        longitude = latitude = altitude = heading = tilt = range_val = altitude_mode = None

    return longitude, latitude, altitude, heading, tilt, range_val, altitude_mode


def extract_style_info(placemark, ns, styles, style_maps, use_highlight):
    """
    Extracts style information from a Placemark, handling both inline <Style> and <styleUrl>.
    """
    color, width, poly_color, poly_opacity, line_opacity = None, None, None, None, None
    icon_color, label_color = None, None
    icon_href, icon_scale = None, None

    # First, handle inline <Style>
    inline_style = placemark.find('kml:Style', ns)
    if inline_style is not None:
        # Extract LineStyle
        line_style = inline_style.find('.//kml:LineStyle', ns)
        if line_style is not None:
            color = line_style.find('kml:color', ns).text if line_style.find('kml:color', ns) is not None else None
            width = line_style.find('kml:width', ns).text if line_style.find('kml:width', ns) is not None else None

        # Extract PolyStyle
        poly_style = inline_style.find('.//kml:PolyStyle', ns)
        if poly_style is not None:
            poly_color = poly_style.find('kml:color', ns).text if poly_style.find('kml:color', ns) is not None else None

        # Extract IconStyle
        icon_style = inline_style.find('.//kml:IconStyle', ns)
        if icon_style is not None:
            icon_scale = icon_style.find('kml:scale', ns).text if icon_style.find('kml:scale', ns) is not None else None
            icon_href = icon_style.find('.//kml:Icon/kml:href', ns).text if icon_style.find('.//kml:Icon/kml:href', ns) is not None else None
            icon_color = icon_style.find('kml:color', ns).text if icon_style.find('kml:color', ns) is not None else None

        # Extract LabelStyle
        label_style = inline_style.find('.//kml:LabelStyle', ns)
        if label_style is not None:
            label_color = label_style.find('kml:color', ns).text if label_style.find('kml:color', ns) is not None else None

        # Calculate opacity from color
        if color:
            alpha_hex = color[:2]
            alpha_decimal = int(alpha_hex, 16)
            line_opacity = (alpha_decimal / 255) * 100

        if poly_color:
            alpha_hex = poly_color[:2]
            alpha_decimal = int(alpha_hex, 16)
            poly_opacity = (alpha_decimal / 255) * 100

    # Now, handle <styleUrl>
    style_url = placemark.find('kml:styleUrl', ns)
    if style_url is not None:
        style_ref = style_url.text.lstrip('#')

        def resolve_style(style_ref_inner, is_highlight_inner):
            if style_ref_inner in style_maps:
                key = 'highlight' if is_highlight_inner else 'normal'
                style_pair = style_maps[style_ref_inner].find(f".//kml:Pair[kml:key='{key}']", ns)
                if style_pair is not None:
                    style_ref_resolved = style_pair.find('kml:styleUrl', ns).text.lstrip('#')
                    return resolve_style(style_ref_resolved, is_highlight_inner)
            return style_ref_inner

        resolved_style_ref = resolve_style(style_ref, use_highlight)
        if resolved_style_ref in styles:
            resolved_style = styles[resolved_style_ref]

            # Extract LineStyle
            line_style = resolved_style.find('.//kml:LineStyle', ns)
            if line_style is not None and color is None and width is None:
                color = line_style.find('kml:color', ns).text if line_style.find('kml:color', ns) is not None else None
                width = line_style.find('kml:width', ns).text if line_style.find('kml:width', ns) is not None else None

            # Extract PolyStyle
            poly_style = resolved_style.find('.//kml:PolyStyle', ns)
            if poly_style is not None and poly_color is None:
                poly_color = poly_style.find('kml:color', ns).text if poly_style.find('kml:color', ns) is not None else None

            # Extract IconStyle
            icon_style = resolved_style.find('.//kml:IconStyle', ns)
            if icon_style is not None and (icon_href is None or icon_scale is None or icon_color is None):
                icon_scale = icon_style.find('kml:scale', ns).text if icon_style.find('kml:scale', ns) is not None else icon_scale
                icon_href = icon_style.find('.//kml:Icon/kml:href', ns).text if icon_style.find('.//kml:Icon/kml:href', ns) is not None else icon_href
                icon_color = icon_style.find('kml:color', ns).text if icon_style.find('kml:color', ns) is not None else icon_color

            # Extract LabelStyle
            label_style = resolved_style.find('.//kml:LabelStyle', ns)
            if label_style is not None and label_color is None:
                label_color = label_style.find('kml:color', ns).text if label_style.find('kml:color', ns) is not None else None

            # Calculate opacity from resolved style
            if color and line_opacity is None:
                alpha_hex = color[:2]
                alpha_decimal = int(alpha_hex, 16)
                line_opacity = (alpha_decimal / 255) * 100

            if poly_color and poly_opacity is None:
                alpha_hex = poly_color[:2]
                alpha_decimal = int(alpha_hex, 16)
                poly_opacity = (alpha_decimal / 255) * 100

    # Log the extracted styles for debugging
    logging.debug(f"Extracted Styles - Line Color: {color}, Line Width: {width}, Poly Color: {poly_color}, "
                  f"Poly Opacity: {poly_opacity}, Icon Href: {icon_href}, Icon Scale: {icon_scale}, "
                  f"Icon Color: {icon_color}, Label Color: {label_color}, Line Opacity: {line_opacity}")

    return color, width, poly_color, poly_opacity, icon_href, icon_scale, icon_color, label_color, line_opacity


def get_folder_element(folder_path, parent_elem, folder_dict):
    """
    Recursively creates or retrieves folder elements based on the folder path.
    """
    if not folder_path:
        return parent_elem

    folders = folder_path.strip().split(' > ')
    current_path = ''
    current_elem = parent_elem

    for folder_name in folders:
        folder_name = folder_name.strip()
        try:
            folder_data = ast.literal_eval(folder_name)
            if isinstance(folder_data, dict):
                if 'featureType' in folder_data:
                    folder_name = folder_data['featureType']
                elif 'name' in folder_data:
                    folder_name = folder_data['name']
                else:
                    folder_name = ', '.join(f'{k}: {v}' for k, v in folder_data.items())
            else:
                folder_name = str(folder_data)
        except (ValueError, SyntaxError):
            pass

        if current_path:
            current_path += '/' + folder_name
        else:
            current_path = folder_name

        if current_path in folder_dict:
            current_elem = folder_dict[current_path]
        else:
            # Decide whether to create a Folder or Document based on naming
            if '.kmz' in folder_name.lower():
                folder_elem = etree.SubElement(current_elem, "Document")
            else:
                folder_elem = etree.SubElement(current_elem, "Folder")
            name_elem = etree.SubElement(folder_elem, "name")
            name_elem.text = folder_name
            folder_dict[current_path] = folder_elem
            current_elem = folder_elem

    return current_elem


def extract_geometry_type(placemark, ns):
    """
    Identifies the geometry type of the placemark.
    """
    if placemark.find('.//kml:MultiGeometry', ns) is not None:
        return 'MultiGeometry'
    elif placemark.find('.//kml:Point', ns) is not None:
        return 'Point'
    elif placemark.find('.//kml:LineString', ns) is not None:
        return 'LineString'
    elif placemark.find('.//kml:Polygon', ns) is not None:
        return 'Polygon'
    else:
        return 'Unknown'


def compute_line_length(placemark, ns):
    """
    Computes the total length of all LineStrings in the placemark.
    """
    total_length = 0.0

    # Find all LineString elements in the placemark
    line_strings = placemark.findall('.//kml:LineString', ns)

    for line_string in line_strings:
        coordinates_element = line_string.find('kml:coordinates', ns)
        if coordinates_element is not None and coordinates_element.text:
            coordinates_list = coordinates_element.text.strip().split()
            # Parse coordinates into list of (lat, lon) tuples
            points = []
            for coord in coordinates_list:
                lon_lat_alt = coord.strip().split(',')
                if len(lon_lat_alt) >= 2:
                    lon = float(lon_lat_alt[0])
                    lat = float(lon_lat_alt[1])
                    # We can ignore altitude for distance calculation
                    points.append((lat, lon))
            # Compute length between consecutive points
            if len(points) >= 2:
                for i in range(len(points) - 1):
                    # Use geodesic distance
                    length = geodesic(points[i], points[i + 1]).meters
                    total_length += length

    return total_length


def extract_placemark_details(placemark, ns, styles, style_maps, use_highlight):
    """
    Extracts detailed information from a Placemark element.
    """
    name = placemark.find('kml:name', ns).text if placemark.find('kml:name', ns) is not None else None
    description_element = placemark.find('kml:description', ns)
    description = description_element.text if description_element is not None else None

    # Extract additional data from description
    additional_data = extract_data_from_description(description)

    # Extract ExtendedData from <ExtendedData> tag
    extended_data = {}
    extended_data_element = placemark.find('kml:ExtendedData', ns)
    if extended_data_element is not None:
        for data_field in extended_data_element.findall('kml:Data', ns):
            data_name = data_field.get('name')
            data_value = data_field.find('kml:value', ns).text if data_field.find('kml:value', ns) is not None else None
            extended_data[data_name] = data_value

    # Extract LookAt or other positional information
    longitude, latitude, altitude, heading, tilt, range_val, altitude_mode = extract_lookat(placemark, ns)

    # Extract coordinates and geometry type
    geometry_type = extract_geometry_type(placemark, ns)

    # Initialize line_length to None
    line_length = None

    # Log the entire Placemark XML for debugging
    placemark_xml = etree.tostring(placemark, pretty_print=True, encoding='unicode')
    logging.debug(f"Placemark XML:\n{placemark_xml}")

    if geometry_type == 'MultiGeometry':
        # Extract the entire MultiGeometry element as a string
        multi_geometry_element = placemark.find('.//kml:MultiGeometry', ns)
        if multi_geometry_element is not None:
            geometry_xml = etree.tostring(multi_geometry_element, encoding='unicode')
            logging.debug(f"Serialized MultiGeometry: {geometry_xml}")
            # Compute the total length of LineStrings in MultiGeometry
            line_length = compute_line_length(placemark, ns)
            logging.debug(f"Computed total line length for MultiGeometry: {line_length} meters")
        else:
            geometry_xml = None
            logging.warning(f"Placemark '{name}' has MultiGeometry type but no MultiGeometry element found.")
        coordinates = None  # Coordinates are not applicable for MultiGeometry
    elif geometry_type == 'LineString':
        # Extract the LineString element and its XML
        geometry_element = placemark.find('.//kml:LineString', ns)
        if geometry_element is not None:
            geometry_xml = etree.tostring(geometry_element, encoding='unicode')
            logging.debug(f"Serialized Geometry (LineString): {geometry_xml}")
            # Compute the length of the LineString
            line_length = compute_line_length(placemark, ns)
            logging.debug(f"Computed line length for LineString: {line_length} meters")
        else:
            geometry_xml = None
            logging.warning(f"Placemark '{name}' has geometry type 'LineString' but no LineString element found.")
        coordinates_element = placemark.find('.//kml:coordinates', ns)
        if coordinates_element is not None and coordinates_element.text is not None:
            coordinates = coordinates_element.text.strip()
        else:
            coordinates = None
    else:
        # Handle other geometry types as before
        # Extract the geometry element and its XML
        geometry_element = placemark.find('.//kml:Point|.//kml:Polygon', ns)
        if geometry_element is not None:
            geometry_xml = etree.tostring(geometry_element, encoding='unicode')
            logging.debug(f"Serialized Geometry ({geometry_type}): {geometry_xml}")
        else:
            geometry_xml = None
            logging.warning(f"Placemark '{name}' has geometry type '{geometry_type}' but no corresponding geometry element found.")
        coordinates_element = placemark.find('.//kml:coordinates', ns)
        if coordinates_element is not None and coordinates_element.text is not None:
            coordinates = coordinates_element.text.strip()
        else:
            coordinates = None

    label_scale = placemark.find('.//gx:drawOrder', ns)
    label_scale = label_scale.text if label_scale is not None else None

    # Extract style information (including the icon style)
    color, width, poly_color, poly_opacity, icon_href, icon_scale, icon_color, label_color, line_opacity = extract_style_info(
        placemark, ns, styles, style_maps, use_highlight)

    # Get folder hierarchy
    folder_hierarchy = []
    parent = placemark.getparent()
    while parent is not None:
        if parent.tag in ('{http://www.opengis.net/kml/2.2}Folder', '{http://www.opengis.net/kml/2.2}Document'):
            folder_name_element = parent.find('kml:name', ns)
            if folder_name_element is not None and folder_name_element.text is not None:
                folder_hierarchy.insert(0, folder_name_element.text)
        parent = parent.getparent()

    # Get attributes
    attributes = dict(placemark.attrib)

    # Logging for debugging
    logging.debug(f"Placemark: {name}")
    logging.debug(f"Geometry Type: {geometry_type}")
    logging.debug(f"Geometry XML: {geometry_xml}")

    placemark_data = {
        'name': name,
        'description': description,
        'coordinates': coordinates,
        'longitude': longitude,
        'latitude': latitude,
        'altitude': altitude,
        'heading': heading,
        'tilt': tilt,
        'range': range_val,
        'altitude_mode': altitude_mode,
        'line_color': color,
        'line_width': width,
        'line_opacity': line_opacity,
        'poly_color': poly_color,
        'poly_opacity': poly_opacity,
        'icon_href': icon_href,
        'icon_scale': icon_scale,
        'icon_color': icon_color,
        'label_color': label_color,
        'label_scale': label_scale,
        'extended_data': extended_data,
        'folder_hierarchy': folder_hierarchy,
        'attributes': attributes,
        'geometry_type': geometry_type,
        'geometry_xml': geometry_xml,
        'line_length': line_length,
        'date_acq': additional_data.get('date_acq'),
        'voltage': additional_data.get('voltage'),
        'cable': additional_data.get('cable'),
        'from_str': additional_data.get('from_str'),
        'to_str': additional_data.get('to_str'),
        'disp_condition': additional_data.get('disp_condition'),
        'five_digit_code': additional_data.get('five_digit_code'),
        'county': additional_data.get('county'),
        'address': additional_data.get('address'),
        'station_voltage': additional_data.get('station_voltage'),
        'gln_x': additional_data.get('gln_x'),
        'gln_y': additional_data.get('gln_y')
    }

    return placemark_data


def extract_groundoverlay_details(groundoverlay, ns):
    """
    Extracts detailed information from a GroundOverlay element.
    """
    name = groundoverlay.find('kml:name', ns).text if groundoverlay.find('kml:name', ns) is not None else None
    visibility = groundoverlay.find('kml:visibility', ns).text if groundoverlay.find('kml:visibility', ns) is not None else None
    color = groundoverlay.find('kml:color', ns).text if groundoverlay.find('kml:color', ns) is not None else None
    icon_href = groundoverlay.find('.//kml:Icon/kml:href', ns).text if groundoverlay.find('.//kml:Icon/kml:href', ns) is not None else None
    rotation = None  # Initialize rotation to ensure it's always defined

    # Initialize view_bound_scale
    view_bound_scale = None

    # Check for gx:LatLonQuad
    latlonquad = groundoverlay.find('.//gx:LatLonQuad', ns)
    if latlonquad is not None:
        coordinates = latlonquad.find('kml:coordinates', ns).text.strip() if latlonquad.find('kml:coordinates', ns) is not None else None
        north = south = east = west = None  # Not applicable when using LatLonQuad
    else:
        # Fallback to LatLonBox
        latlonbox = groundoverlay.find('kml:LatLonBox', ns)
        if latlonbox is not None:
            north = latlonbox.find('kml:north', ns).text if latlonbox.find('kml:north', ns) is not None else None
            south = latlonbox.find('kml:south', ns).text if latlonbox.find('kml:south', ns) is not None else None
            east = latlonbox.find('kml:east', ns).text if latlonbox.find('kml:east', ns) is not None else None
            west = latlonbox.find('kml:west', ns).text if latlonbox.find('kml:west', ns) is not None else None
            coordinates = None  # No LatLonQuad; use individual values instead

            # Correctly Extract rotation from LatLonBox
            rotation_elem = latlonbox.find('kml:rotation', ns)
            if rotation_elem is not None and rotation_elem.text:
                try:
                    rotation = float(rotation_elem.text)
                    logging.debug(f"Extracted rotation: {rotation}")
                except ValueError:
                    logging.warning(f"Invalid rotation value: {rotation_elem.text}")
            else:
                rotation = None
        else:
            north = south = east = west = coordinates = rotation = None

    # Extract viewBoundScale from <Icon>
    icon = groundoverlay.find('kml:Icon', ns)
    if icon is not None:
        view_bound_scale_elem = icon.find('kml:viewBoundScale', ns)
        if view_bound_scale_elem is not None and view_bound_scale_elem.text:
            try:
                view_bound_scale = float(view_bound_scale_elem.text)
                logging.debug(f"Extracted viewBoundScale: {view_bound_scale}")
            except ValueError:
                logging.warning(f"Invalid viewBoundScale value: {view_bound_scale_elem.text}")

    # Extract ExtendedData for GroundOverlay
    extended_data = {}
    extended_data_element = groundoverlay.find('kml:ExtendedData', ns)
    if extended_data_element is not None:
        for data_field in extended_data_element.findall('kml:Data', ns):
            data_name = data_field.get('name')
            data_value = data_field.find('kml:value', ns).text if data_field.find('kml:value', ns) is not None else None
            extended_data[data_name] = data_value

    # Get folder hierarchy
    folder_hierarchy = []
    parent = groundoverlay.getparent()
    while parent is not None:
        if parent.tag in ('{http://www.opengis.net/kml/2.2}Folder', '{http://www.opengis.net/kml/2.2}Document'):
            folder_name_element = parent.find('kml:name', ns)
            if folder_name_element is not None and folder_name_element.text is not None:
                folder_hierarchy.insert(0, folder_name_element.text)
        parent = parent.getparent()

    # Get attributes
    attributes = dict(groundoverlay.attrib)

    # Logging for debugging
    logging.debug(f"GroundOverlay: {name}")

    return {
        'name': name,
        'visibility': visibility,
        'color': color,
        'icon_href': icon_href,
        'coordinates': coordinates,  # Use for LatLonQuad coordinates
        'north': north,  # Use for LatLonBox values
        'south': south,
        'east': east,
        'west': west,
        'rotation': rotation,  # Correctly extracted rotation
        'view_bound_scale': view_bound_scale,  # New field extracted here
        'folder_hierarchy': folder_hierarchy,
        'attributes': attributes,
        'extended_data': extended_data  # Include extended_data
    }


def extract_networklink_details(networklink, ns):
    """
    Extracts detailed information from a NetworkLink element.
    """
    name = networklink.find('kml:name', ns).text if networklink.find('kml:name', ns) is not None else None
    visibility = networklink.find('kml:visibility', ns).text if networklink.find('kml:visibility', ns) is not None else None

    # Extract LookAt or other positional information
    longitude, latitude, altitude, heading, tilt, range_val, altitude_mode = extract_lookat(networklink, ns)

    # Handle both <Link> and <Url>
    link = networklink.find('kml:Link', ns)
    if link is None:  # If <Link> is not found, try <Url>
        link = networklink.find('kml:Url', ns)

    if link is not None:
        href = link.find('kml:href', ns).text if link.find('kml:href', ns) is not None else None
        viewRefreshMode = link.find('kml:viewRefreshMode', ns).text if link.find('kml:viewRefreshMode', ns) is not None else None
        viewRefreshTime = link.find('kml:viewRefreshTime', ns).text if link.find('kml:viewRefreshTime', ns) is not None else None
    else:
        href = viewRefreshMode = viewRefreshTime = None

    # Extract ExtendedData for NetworkLink
    extended_data = {}
    extended_data_element = networklink.find('kml:ExtendedData', ns)
    if extended_data_element is not None:
        for data_field in extended_data_element.findall('kml:Data', ns):
            data_name = data_field.get('name')
            data_value = data_field.find('kml:value', ns).text if data_field.find('kml:value', ns) is not None else None
            extended_data[data_name] = data_value

    # Get folder hierarchy
    folder_hierarchy = []
    parent = networklink.getparent()
    while parent is not None:
        if parent.tag in ('{http://www.opengis.net/kml/2.2}Folder', '{http://www.opengis.net/kml/2.2}Document'):
            folder_name_element = parent.find('kml:name', ns)
            if folder_name_element is not None and folder_name_element.text is not None:
                folder_hierarchy.insert(0, folder_name_element.text)
        parent = parent.getparent()

    # Get attributes
    attributes = dict(networklink.attrib)

    # Logging for debugging
    logging.debug(f"NetworkLink: {name}")

    return {
        'name': name,
        'visibility': visibility,
        'longitude': longitude,
        'latitude': latitude,
        'altitude': altitude,
        'heading': heading,
        'tilt': tilt,
        'range': range_val,
        'altitude_mode': altitude_mode,
        'href': href,  # Correctly extract href from either Link or Url
        'viewRefreshMode': viewRefreshMode,  # Correctly extract viewRefreshMode
        'viewRefreshTime': viewRefreshTime,  # Correctly extract viewRefreshTime
        'extended_data': extended_data,
        'folder_hierarchy': folder_hierarchy,
        'attributes': attributes
    }


def write_to_output(data, groundoverlays, networklinks, output_file):
    """
    Writes the extracted data to an output text file for verification.
    """
    with open(output_file, 'w', encoding='utf-8') as file:
        for placemark_data in data:
            file.write(f"Placemark Name: {placemark_data['name']}\n")
            file.write(f"Coordinates: {placemark_data['coordinates']}\n")
            file.write(f"Longitude: {placemark_data['longitude']}\n")
            file.write(f"Latitude: {placemark_data['latitude']}\n")
            file.write(f"Altitude: {placemark_data['altitude']}\n")
            file.write(f"Heading: {placemark_data['heading']}\n")
            file.write(f"Tilt: {placemark_data['tilt']}\n")
            file.write(f"Range: {placemark_data['range']}\n")
            file.write(f"Altitude Mode: {placemark_data['altitude_mode']}\n")
            file.write(f"Line Color: {placemark_data['line_color']}\n")
            file.write(f"Line Width: {placemark_data['line_width']}\n")
            file.write(f"Line Opacity: {placemark_data['line_opacity']}\n")
            file.write(f"Poly Color: {placemark_data['poly_color']}\n")
            file.write(f"Poly Opacity: {placemark_data['poly_opacity']}\n")
            file.write(f"Icon URL: {placemark_data['icon_href']}\n")
            file.write(f"Icon Scale: {placemark_data['icon_scale']}\n")
            file.write(f"Icon Color: {placemark_data['icon_color']}\n")
            file.write(f"Label Color: {placemark_data['label_color']}\n")
            file.write(f"Label Scale: {placemark_data['label_scale']}\n")
            file.write(f"Description: {placemark_data['description']}\n")
            file.write(f"Line Length: {placemark_data['line_length']}\n")  # Add line length

            # Write extended data fields (if any)
            if placemark_data['extended_data']:
                file.write("Extended Data:\n")
                for key, value in placemark_data['extended_data'].items():
                    file.write(f"  {key}: {value}\n")

            # Write additional data extracted from description
            if any([
                placemark_data.get('date_acq'),
                placemark_data.get('voltage'),
                placemark_data.get('cable'),
                placemark_data.get('from_str'),
                placemark_data.get('to_str'),
                placemark_data.get('disp_condition'),
                placemark_data.get('five_digit_code'),
                placemark_data.get('county'),
                placemark_data.get('address'),
                placemark_data.get('station_voltage'),
                placemark_data.get('gln_x'),
                placemark_data.get('gln_y')
            ]):
                file.write("Additional Data from Description:\n")
                if placemark_data.get('date_acq'):
                    file.write(f"  Date Acquired: {placemark_data['date_acq']}\n")
                if placemark_data.get('voltage'):
                    file.write(f"  Voltage: {placemark_data['voltage']}\n")
                if placemark_data.get('cable'):
                    file.write(f"  Cable/Conductor Type: {placemark_data['cable']}\n")
                if placemark_data.get('from_str'):
                    file.write(f"  From Str.: {placemark_data['from_str']}\n")
                if placemark_data.get('to_str'):
                    file.write(f"  To Str.: {placemark_data['to_str']}\n")
                if placemark_data.get('disp_condition'):
                    file.write(f"  Disp. Condition: {placemark_data['disp_condition']}\n")
                if placemark_data.get('five_digit_code'):
                    file.write(f"  5 Digit Code: {placemark_data['five_digit_code']}\n")
                if placemark_data.get('county'):
                    file.write(f"  County: {placemark_data['county']}\n")
                if placemark_data.get('address'):
                    file.write(f"  Address: {placemark_data['address']}\n")
                if placemark_data.get('station_voltage'):
                    file.write(f"  Station Voltage: {placemark_data['station_voltage']}\n")
                if placemark_data.get('gln_x'):
                    file.write(f"  GLN X: {placemark_data['gln_x']}\n")
                if placemark_data.get('gln_y'):
                    file.write(f"  GLN Y: {placemark_data['gln_y']}\n")

            # Write folder hierarchy
            if placemark_data['folder_hierarchy']:
                file.write(f"Folder Hierarchy: {' > '.join(placemark_data['folder_hierarchy'])}\n")

            # Write attributes
            if placemark_data['attributes']:
                file.write("Attributes:\n")
                for attr_name, attr_value in placemark_data['attributes'].items():
                    file.write(f"  {attr_name}: {attr_value}\n")

            # Optionally write geometry XML
            if placemark_data['geometry_xml']:
                file.write(f"Geometry XML: {placemark_data['geometry_xml']}\n")

            file.write('*** ------------------------------------------------- ***\n\n')

        # GroundOverlays and NetworkLinks writing remains the same
        # ...


def parse_kml(kml_file, conn, use_highlight=False):
    """
    Parses the KML file and inserts data into the database.
    """
    logging.info(f"Parsing .kml file: {kml_file}")

    # Fix the namespace if necessary
    corrected_kml_file_path = fix_kml_namespace(kml_file)

    try:
        tree = etree.parse(corrected_kml_file_path)
    except etree.XMLSyntaxError as e:
        logging.error(f"Failed to parse KML file: {e}")
        return [], [], []

    root = tree.getroot()
    ns = {'kml': 'http://www.opengis.net/kml/2.2', 'gx': 'http://www.google.com/kml/ext/2.2'}

    # Build dictionaries of styles and style maps for quick lookup
    styles, style_maps = parse_styles_and_maps(root, ns)

    placemarks = root.findall('.//kml:Placemark', ns)
    groundoverlays = root.findall('.//kml:GroundOverlay', ns)

    # Find NetworkLink elements
    networklinks = root.findall('.//kml:NetworkLink', ns)

    data = []
    for placemark in placemarks:
        placemark_data = extract_placemark_details(placemark, ns, styles, style_maps, use_highlight)
        logging.debug(f"Extracted Placemark Data: {placemark_data}")
        insert_placemark(conn, placemark_data)
        data.append(placemark_data)

    groundoverlay_data = []
    for overlay in groundoverlays:
        overlay_data = extract_groundoverlay_details(overlay, ns)
        insert_groundoverlay(conn, overlay_data)
        groundoverlay_data.append(overlay_data)

    networklink_data_list = []
    for networklink in networklinks:
        networklink_data = extract_networklink_details(networklink, ns)
        insert_networklink(conn, networklink_data)
        networklink_data_list.append(networklink_data)
        logging.debug(f"Inserted NetworkLink: {networklink_data['name']}")

    # Optional: Write to a text file for verification
    # output_file_path = os.path.join(os.path.dirname(kml_file), 'parsed_output.txt')
    # write_to_output(data, groundoverlay_data, networklink_data_list, output_file_path)

    logging.info(f"Parsed {len(placemarks)} placemarks, {len(groundoverlays)} ground overlays, and {len(networklinks)} network links.")

    return data, groundoverlay_data, networklink_data_list


def create_kmz_with_images(kml_file, kmz_file, source_folder):
    """
    Creates a KMZ file from a KML file and its associated resources (images).
    """
    try:
        with zipfile.ZipFile(kmz_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Add the KML file to the KMZ
            zf.write(kml_file, os.path.basename(kml_file))

            # Add referenced files (e.g., images in the 'files' folder)
            for root_dir, dirs, files in os.walk(source_folder):
                for file in files:
                    file_path = os.path.join(root_dir, file)
                    # Compute the archive name relative to source_folder
                    arcname = os.path.relpath(file_path, source_folder)
                    zf.write(file_path, arcname)

        logging.info(f"KMZ file created at: {kmz_file}")
    except Exception as e:
        logging.error(f"Failed to create KMZ file: {e}")


def is_valid_number(value):
    """
    Utility function to check if a value can be converted to a float.
    """
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def add_extended_data(element, extended_data_str):
    """
    Adds ExtendedData to a KML element based on a string representation of a dictionary.
    """
    try:
        extended_data = ast.literal_eval(extended_data_str)
        if isinstance(extended_data, dict):
            extended_data_elem = etree.SubElement(element, "ExtendedData")
            for key, value in extended_data.items():
                data_elem = etree.SubElement(extended_data_elem, "Data", name=key)
                value_elem = etree.SubElement(data_elem, "value")
                value_elem.text = str(value) if value is not None else ""
        else:
            logging.info(f"No valid ExtendedData to write for: {extended_data_str}")
    except (ValueError, SyntaxError) as e:
        logging.warning(f"Invalid extended_data format: {extended_data_str}. Error: {e}")


def copy_images_to_output(source_folder, destination_folder):
    """
    Copies all image files from the source folder to the destination folder.
    """
    if os.path.exists(source_folder):
        # Ensure the destination folder exists
        os.makedirs(destination_folder, exist_ok=True)

        # Copy each image file from the source folder to the destination folder
        for item in os.listdir(source_folder):
            source_file = os.path.join(source_folder, item)
            if os.path.isfile(source_file):
                shutil.copy2(source_file, destination_folder)  # Copy files with metadata
                logging.info(f"Copied image: {source_file} to {destination_folder}")
    else:
        logging.warning(f"Source folder {source_folder} does not exist. No images to copy.")


def extract_kml(kmz_file, extract_path):
    """
    Extracts and fixes the KML file from a KMZ archive.
    """
    kml_file = extract_kml_from_kmz(kmz_file, extract_path)
    if kml_file:
        corrected_kml_file = fix_kml_namespace(kml_file)
        return corrected_kml_file
    else:
        logging.error("No .kml file found in the .kmz archive")
        return None



# The main function is adjusted to remove the db_path parameter
if __name__ == "__main__":
    current_dir = os.getcwd()  # Get the current directory

    # Search for the only .kmz file in the current folder
    kmz_files = glob.glob(os.path.join(current_dir, '*.kmz'))  # Find all .kmz files in the current directory

    if len(kmz_files) == 1:
        kmz_file = kmz_files[0]  # Use the only found .kmz file
    else:
        raise FileNotFoundError("Either no .kmz files or multiple .kmz files found in the current directory. Ensure there is exactly one .kmz file.")

    # Paths for extraction
    extract_path = os.path.join(current_dir, 'outputs')  # Path for extraction

    # Ensure the output directory exists
    os.makedirs(extract_path, exist_ok=True)
    images_folder = os.path.join(extract_path, 'images')
    os.makedirs(images_folder, exist_ok=True)  # Create the images folder if it doesn't exist

    # Initialize the database connection
    conn = init_db()

    # Extract KML from KMZ
    kml_file = extract_kml(kmz_file, extract_path)

    if kml_file:
        # Parse the KML and populate the database
        placemarks, groundoverlays, networklinks = parse_kml(kml_file, conn, use_highlight=True)

        # Copy images to the output folder
        source_folder = os.path.join(current_dir, 'outputs/files')  # Source folder for original images/resources
        copy_images_to_output(source_folder, images_folder)
    else:
        logging.error("No .kml file found in the .kmz archive")
