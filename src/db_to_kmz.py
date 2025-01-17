import zipfile
import os
from lxml import etree
import pyodbc
import ast
import logging
import shutil
import random
import re
from datetime import datetime
from PIL import Image  # Import Pillow for image processing
import io  # For in-memory file handling
from geopy.distance import geodesic
import math
import matplotlib.pyplot as plt  # For visualization
import matplotlib.cm as cm
import matplotlib.colors as mcolors
from collections import defaultdict
import argparse

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for detailed logs
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler("reconstruction.log"),
        logging.StreamHandler()
    ]
)

# Database connection parameters
server_name = 'sql_server,1433'
database_name = 'your_database_name'
username = 'sa'
password = 'YourStrong!Password'

proximity_threshold = 10  # meters
angle_threshold = 3       # degrees
GRID_SIZE_DEGREES = 0.001

class Placemark:
    def __init__(self, row):
        self.name = row['name']
        self.description = row['description']
        self.geometry_type = row['geometry_type']
        self.line_strings = []  # List to store LineStrings
        self.parse_geometry(row)

    def parse_geometry(self, row):
        if self.geometry_type == 'LineString':
            coords = self.parse_coordinates(row['coordinates'])
            if coords:
                self.line_strings.append(coords)
        elif self.geometry_type == 'MultiGeometry':
            geometry_xml_str = row['geometry_xml']
            if geometry_xml_str:
                try:
                    geometry_xml = etree.fromstring(geometry_xml_str.encode('utf-8'))
                    line_string_elements = geometry_xml.findall(".//{http://www.opengis.net/kml/2.2}LineString")
                    for line_string_elem in line_string_elements:
                        coord_elem = line_string_elem.find("{http://www.opengis.net/kml/2.2}coordinates")
                        if coord_elem is not None and coord_elem.text:
                            coords = self.parse_coordinates(coord_elem.text)
                            if coords:
                                self.line_strings.append(coords)
                except etree.XMLSyntaxError as e:
                    logging.error(f"XML parsing error for Placemark '{self.name}': {e}")

    def parse_coordinates(self, coord_str):
        coords = []
        if coord_str:
            for coord in coord_str.strip().split():
                parts = coord.strip().split(',')
                if len(parts) >= 2:
                    try:
                        lon = float(parts[0])
                        lat = float(parts[1])
                        alt = float(parts[2]) if len(parts) > 2 else 0.0
                        coords.append((lat, lon, alt))
                    except ValueError as e:
                        logging.warning(f"Invalid coordinate format in Placemark '{self.name}': {coord} - {e}")
        return coords

    def get_line_segments(self):
        segments = []
        for coords in self.line_strings:
            for i in range(len(coords) - 1):
                lat1, lon1, alt1 = coords[i]
                lat2, lon2, alt2 = coords[i + 1]
                segments.append(((lat1, lon1, alt1), (lat2, lon2, alt2)))
        return segments

def calculate_3d_distance(coord1, coord2):
    lat1, lon1, alt1 = coord1
    lat2, lon2, alt2 = coord2
    surface_distance = geodesic((lat1, lon1), (lat2, lon2)).meters
    altitude_diff = alt2 - alt1
    distance_3d = math.sqrt(surface_distance ** 2 + altitude_diff ** 2)
    return distance_3d

def calculate_bearing(coord1, coord2):
    lat1, lon1 = math.radians(coord1[0]), math.radians(coord1[1])
    lat2, lon2 = math.radians(coord2[0]), math.radians(coord2[1])
    d_lon = lon2 - lon1

    x = math.sin(d_lon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - \
        math.sin(lat1) * math.cos(lat2) * math.cos(d_lon)

    initial_bearing = math.atan2(x, y)
    bearing = (math.degrees(initial_bearing) + 360) % 360
    return bearing

def get_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server_name};"
        f"DATABASE={database_name};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt=no;"
    )
    return pyodbc.connect(conn_str)

def ensure_tables_exist(conn):
    cursor = conn.cursor()

    # placemarks
    cursor.execute('''
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='placemarks' AND xtype='U')
    BEGIN
        CREATE TABLE placemarks (
            id INT IDENTITY PRIMARY KEY,
            name NVARCHAR(MAX),
            description NVARCHAR(MAX),
            coordinates NVARCHAR(MAX),
            longitude FLOAT,
            latitude FLOAT,
            altitude FLOAT,
            heading FLOAT,
            tilt FLOAT,
            [range] FLOAT,
            altitude_mode NVARCHAR(100),
            line_color NVARCHAR(100),
            line_width FLOAT,
            line_opacity FLOAT,
            poly_color NVARCHAR(100),
            poly_opacity FLOAT,
            icon_href NVARCHAR(MAX),
            icon_scale FLOAT,
            icon_color NVARCHAR(100),
            label_color NVARCHAR(100),
            label_scale NVARCHAR(100),
            extended_data NVARCHAR(MAX),
            folder_hierarchy NVARCHAR(MAX),
            attributes NVARCHAR(MAX),
            geometry_type NVARCHAR(100),
            geometry_xml NVARCHAR(MAX),
            cable NVARCHAR(MAX),
            voltage NVARCHAR(MAX),
            date_acq NVARCHAR(MAX),
            line_length FLOAT
        )
    END
    ''')

    # groundoverlays
    cursor.execute('''
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='groundoverlays' AND xtype='U')
    BEGIN
        CREATE TABLE groundoverlays (
            id INT IDENTITY PRIMARY KEY,
            name NVARCHAR(MAX),
            visibility INT,
            color NVARCHAR(100),
            icon_href NVARCHAR(MAX),
            coordinates NVARCHAR(MAX),
            north FLOAT,
            south FLOAT,
            east FLOAT,
            west FLOAT,
            rotation FLOAT,
            view_bound_scale FLOAT,
            folder_hierarchy NVARCHAR(MAX),
            attributes NVARCHAR(MAX),
            extended_data NVARCHAR(MAX),
            longitude FLOAT,
            latitude FLOAT,
            altitude FLOAT,
            heading FLOAT,
            tilt FLOAT,
            [range] FLOAT,
            altitude_mode NVARCHAR(100),
            date_acq NVARCHAR(MAX)
        )
    END
    ''')

    # networklinks
    cursor.execute('''
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='networklinks' AND xtype='U')
    BEGIN
        CREATE TABLE networklinks (
            id INT IDENTITY PRIMARY KEY,
            name NVARCHAR(MAX),
            visibility INT,
            longitude FLOAT,
            latitude FLOAT,
            altitude FLOAT,
            heading FLOAT,
            tilt FLOAT,
            [range] FLOAT,
            altitude_mode NVARCHAR(100),
            href NVARCHAR(MAX),
            viewRefreshMode NVARCHAR(100),
            viewRefreshTime FLOAT,
            folder_hierarchy NVARCHAR(MAX),
            attributes NVARCHAR(MAX),
            extended_data NVARCHAR(MAX),
            date_acq NVARCHAR(MAX)
        )
    END
    ''')

    # conductor_types
    cursor.execute('''
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='conductor_types' AND xtype='U')
    BEGIN
        CREATE TABLE conductor_types (
            id INT IDENTITY PRIMARY KEY,
            type NVARCHAR(255) UNIQUE,
            width_mm FLOAT
        )
    END
    ''')

    conn.commit()

def fetch_table(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    # Convert rows to list of dicts
    columns = [desc[0] for desc in cursor.description]
    result = []
    for r in rows:
        row_dict = {}
        for col_index, col_name in enumerate(columns):
            row_dict[col_name] = r[col_index]
        result.append(row_dict)
    return result

def fetch_placemarks(conn):
    return fetch_table(conn, "placemarks")

def fetch_groundoverlays(conn):
    return fetch_table(conn, "groundoverlays")

def fetch_networklinks(conn):
    return fetch_table(conn, "networklinks")

def is_valid_number(value):
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False

def add_extended_data(element, extended_data_str, nsmap):
    if not extended_data_str:
        return
    try:
        extended_data_dict = ast.literal_eval(extended_data_str)
        if isinstance(extended_data_dict, dict) and extended_data_dict:
            extended_data = etree.SubElement(element, "{%s}ExtendedData" % nsmap['kml'])
            for key, value in extended_data_dict.items():
                data_field = etree.SubElement(extended_data, "{%s}Data" % nsmap['kml'], name=str(key))
                value_elem = etree.SubElement(data_field, "{%s}value" % nsmap['kml'])
                value_elem.text = str(value) if value is not None else ""
        else:
            logging.info(f"No valid ExtendedData to write for: {extended_data_str}")
    except (ValueError, SyntaxError) as e:
        logging.warning(f"Invalid extended_data format: {extended_data_str}. Error: {e}")

def get_folder_element(folder_path, parent_elem, folder_dict, nsmap):
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
            if '.kmz' in folder_name.lower():
                folder_elem = etree.SubElement(current_elem, "{%s}Document" % nsmap['kml'])
            else:
                folder_elem = etree.SubElement(current_elem, "{%s}Folder" % nsmap['kml'])
            name_elem = etree.SubElement(folder_elem, "{%s}name" % nsmap['kml'])
            name_elem.text = folder_name
            folder_dict[current_path] = folder_elem
            current_elem = folder_elem

    return current_elem

def map_voltage_to_color(voltage):
    try:
        if isinstance(voltage, str) and voltage.lower().endswith("kv"):
            voltage = float(voltage[:-2]) * 1000
        else:
            voltage = float(voltage)
    except (TypeError, ValueError):
        return "ffffffff"

    min_voltage = 10
    max_voltage = 3000
    voltage = max(min_voltage, min(max_voltage, voltage))
    normalized = (voltage - min_voltage) / (max_voltage - min_voltage)

    red = int(128 + normalized * (255 - 128))
    green = int(128 * (1 - normalized))
    blue = int(255 * (1 - normalized) + 128 * normalized)

    color = f"ff{blue:02x}{green:02x}{red:02x}"
    return color

def compress_image(image_path, max_size=(1024, 1024), quality=85):
    try:
        with Image.open(image_path) as img:
            original_format = img.format
            original_size = os.path.getsize(image_path)
            logging.debug(f"Original image size: {original_size} bytes for {image_path}")
            img.thumbnail(max_size, Image.LANCZOS)
            img_byte_arr = io.BytesIO()
            if original_format == 'JPEG':
                img.save(img_byte_arr, format='JPEG', quality=quality, optimize=True)
            elif original_format == 'PNG':
                img.save(img_byte_arr, format='PNG', optimize=True)
            else:
                img = img.convert('RGB')
                img.save(img_byte_arr, format='JPEG', quality=quality, optimize=True)
                original_format = 'JPEG'
            compressed_data = img_byte_arr.getvalue()
            compressed_size = len(compressed_data)
            logging.debug(f"Compressed image size: {compressed_size} bytes for {image_path}")
            return compressed_data, original_format
    except Exception as e:
        logging.error(f"Failed to compress image {image_path}: {e}")
        return None, None

def get_grid_cell(lat, lon, grid_size=GRID_SIZE_DEGREES):
    lat_cell = int(lat // grid_size)
    lon_cell = int(lon // grid_size)
    return (lat_cell, lon_cell)

def get_neighboring_cells(cell):
    lat, lon = cell
    neighbors = []
    for dlat in [-1, 0, 1]:
        for dlon in [-1, 0, 1]:
            neighbors.append((lat + dlat, lon + dlon))
    return neighbors

def build_spatial_index_with_names(placemark_objects, grid_size=GRID_SIZE_DEGREES):
    grid_index = defaultdict(list)
    for placemark in placemark_objects:
        segments = placemark.get_line_segments()
        for seg in segments:
            lat_min = min(seg[0][0], seg[1][0])
            lat_max = max(seg[0][0], seg[1][0])
            lon_min = min(seg[0][1], seg[1][1])
            lon_max = max(seg[0][1], seg[1][1])

            lat_cell_min = int(lat_min // grid_size)
            lat_cell_max = int(lat_max // grid_size)
            lon_cell_min = int(lon_min // grid_size)
            lon_cell_max = int(lon_max // grid_size)

            for lat_cell in range(lat_cell_min, lat_cell_max + 1):
                for lon_cell in range(lon_cell_min, lon_cell_max + 1):
                    grid_index[(lat_cell, lon_cell)].append((seg, placemark.name))
    return grid_index

def find_identified_pairs(grid_index, proximity_threshold=10, angle_threshold=3):
    identified_pairs = set()
    processed_pairs = set()

    for cell, segments in grid_index.items():
        neighboring_cells = get_neighboring_cells(cell)
        for neighbor in neighboring_cells:
            if neighbor not in grid_index:
                continue
            neighbor_segments = grid_index[neighbor]

            for seg1, name1 in segments:
                for seg2, name2 in neighbor_segments:
                    if seg1 == seg2 and name1 == name2:
                        continue
                    pair_id = tuple(sorted([(name1, seg1), (name2, seg2)]))
                    if pair_id in processed_pairs:
                        continue
                    processed_pairs.add(pair_id)

                    mid1_lat = (seg1[0][0] + seg1[1][0]) / 2
                    mid1_lon = (seg1[0][1] + seg1[1][1]) / 2
                    mid1_alt = (seg1[0][2] + seg1[1][2]) / 2
                    mid1 = (mid1_lat, mid1_lon, mid1_alt)

                    mid2_lat = (seg2[0][0] + seg2[1][0]) / 2
                    mid2_lon = (seg2[0][1] + seg2[1][1]) / 2
                    mid2_alt = (seg2[0][2] + seg2[1][2]) / 2
                    mid2 = (mid2_lat, mid2_lon, mid2_alt)

                    distance = calculate_3d_distance(mid1, mid2)
                    if distance > proximity_threshold:
                        continue

                    bearing1 = calculate_bearing(seg1[0], seg1[1])
                    bearing2 = calculate_bearing(seg2[0], seg2[1])
                    angle_diff = abs(bearing1 - bearing2)
                    angle_diff = min(angle_diff, 360 - angle_diff)

                    if angle_diff > angle_threshold:
                        continue

                    seg1_tuple = (tuple(seg1[0]), tuple(seg1[1]))
                    seg2_tuple = (tuple(seg2[0]), tuple(seg2[1]))
                    identified_pairs.add((name1, name2, seg1_tuple, seg2_tuple, distance, angle_diff))

    return identified_pairs

def plot_grids_and_lines(grid_index, identified_pairs, output_plot='outputs/grid_plot.pdf'):
    plt.figure(figsize=(24, 24))
    ax = plt.gca()

    grid_cells = grid_index.keys()
    for cell in grid_cells:
        lat_cell, lon_cell = cell
        lat_start = lat_cell * GRID_SIZE_DEGREES
        lon_start = lon_cell * GRID_SIZE_DEGREES
        lat_end = lat_start + GRID_SIZE_DEGREES
        lon_end = lon_start + GRID_SIZE_DEGREES
        rect = plt.Rectangle((lon_start, lat_start), GRID_SIZE_DEGREES, GRID_SIZE_DEGREES,
                             linewidth=0.5, edgecolor='gray', facecolor='none')
        ax.add_patch(rect)

    for segments in grid_index.values():
        for seg, _ in segments:
            latitudes = [seg[0][0], seg[1][0]]
            longitudes = [seg[0][1], seg[1][1]]
            plt.plot(longitudes, latitudes, color='blue', linewidth=0.5, alpha=0.5)

    num_pairs = len(identified_pairs)
    if num_pairs == 0:
        logging.info("No identified pairs to plot.")
    else:
        cmap = cm.get_cmap('tab20', num_pairs)
        for idx, pair in enumerate(identified_pairs):
            _, _, seg1, seg2, _, _ = pair
            color = cmap(idx % cmap.N)
            color_hex = mcolors.to_hex(color)
            plt.plot([seg1[0][1], seg1[1][1]], [seg1[0][0], seg1[1][0]], color=color_hex, linewidth=1.5)
            plt.plot([seg2[0][1], seg2[1][1]], [seg2[0][0], seg2[1][0]], color=color_hex, linewidth=1.5)

    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title('Grid Cells and Line Segments with Identified Pairs')
    plt.grid(False)
    plt.savefig(output_plot, dpi=300)
    plt.close()
    logging.info(f"Grid and lines plot saved to {output_plot}")

def get_conductor_width(conn, conductor_type, cable_field):
    cursor = conn.cursor()
    cursor.execute("SELECT width_mm FROM conductor_types WHERE type = ?", (conductor_type,))
    result = cursor.fetchone()
    if result and result[0] is not None:
        return result[0]
    else:
        width = None
        if cable_field:
            match = re.search(r'(\d+)\s*mm²', cable_field)
            if match:
                width = float(match.group(1))
        if width is None:
            width = random.uniform(1, 100)
            try:
                cursor.execute("INSERT INTO conductor_types (type, width_mm) VALUES (?, ?)", (conductor_type, width))
                conn.commit()
                logging.debug(f"Assigned random width {width:.2f} mm to conductor type '{conductor_type}'")
            except pyodbc.IntegrityError:
                cursor.execute("SELECT width_mm FROM conductor_types WHERE type = ?", (conductor_type,))
                result = cursor.fetchone()
                if result and result[0] is not None:
                    width = result[0]
        logging.debug(f"Width for conductor type '{conductor_type}': {width:.2f} mm")
        return width

def sanitize_icon_href_for_groundoverlays(icon_href):
    if not icon_href:
        return ''
    filename = os.path.basename(icon_href)
    return f"files/{filename}"

def reconstruct_kml(db_path, output_kml, find_pairs=True):
    logging.info("Starting KML reconstruction...")

    # Connect to SQL Server
    conn = get_connection()
    ensure_tables_exist(conn)

    try:
        placemarks = fetch_placemarks(conn)
        groundoverlays = fetch_groundoverlays(conn)
        networklinks = fetch_networklinks(conn)
    except pyodbc.Error as e:
        logging.error(f"Database fetch error: {e}")
        conn.close()
        return

    nsmap = {
        'kml': "http://www.opengis.net/kml/2.2",
        'gx': "http://www.google.com/kml/ext/2.2"
    }

    kml_root = etree.Element("{%s}kml" % nsmap['kml'], nsmap=nsmap)
    document = etree.SubElement(kml_root, "{%s}Document" % nsmap['kml'])
    folder_dict = {}

    # Ensure conductor_types table exists (already done in ensure_tables_exist)

    for row in placemarks:
        if 'description' not in row or row['description'] is None:
            # Uncomment the next line if you want to skip placemarks without description
            # continue
            pass

        folder_hierarchy = row['folder_hierarchy']
        folder_elem = get_folder_element(folder_hierarchy, document, folder_dict, nsmap) if folder_hierarchy else document
        placemark_attributes = {}
        if row['attributes']:
            try:
                placemark_attributes = ast.literal_eval(row['attributes'])
            except:
                pass
        placemark_id = placemark_attributes.get('id')

        if placemark_id:
            placemark = etree.SubElement(folder_elem, "{%s}Placemark" % nsmap['kml'], id=str(placemark_id))
        else:
            placemark = etree.SubElement(folder_elem, "{%s}Placemark" % nsmap['kml'])

        name_elem = etree.SubElement(placemark, "{%s}name" % nsmap['kml'])
        name_elem.text = row['name'] if row['name'] else "Unnamed Placemark"

        description_elem = etree.SubElement(placemark, "{%s}description" % nsmap['kml'])
        base_description = row['description'] if row['description'] else ""
        line_length = row['line_length'] if 'line_length' in row and row['line_length'] is not None else None

        if line_length is not None:
            line_length_str = f"<br/><b>Line Length:</b> {line_length} meters"
            description_elem.text = base_description + line_length_str
            logging.debug(f"Added line_length to description for Placemark '{row['name']}'")
        else:
            description_elem.text = base_description

        geometry_type = row['geometry_type']
        geometry_xml_str = row['geometry_xml']

        if geometry_type == 'MultiGeometry' and geometry_xml_str:
            try:
                geometry_xml = etree.fromstring(geometry_xml_str.encode('utf-8'))
                if not etree.QName(geometry_xml).namespace:
                    geometry_xml.tag = "{%s}%s" % (nsmap['kml'], etree.QName(geometry_xml).localname)
                for elem in geometry_xml.iter():
                    if not etree.QName(elem).namespace:
                        elem.tag = "{%s}%s" % (nsmap['kml'], etree.QName(elem).localname)
                placemark.append(geometry_xml)
                logging.debug(f"Appended MultiGeometry to Placemark '{row['name']}'")
            except etree.XMLSyntaxError as e:
                logging.error(f"Invalid geometry_xml for Placemark '{row['name']}': {e}")
        elif 'coordinates' in row and row['coordinates'] and row['coordinates'] != 'None':
            coordinates = row['coordinates']
            if geometry_type == 'Polygon':
                polygon = etree.SubElement(placemark, "{%s}Polygon" % nsmap['kml'])
                outer_boundary = etree.SubElement(polygon, "{%s}outerBoundaryIs" % nsmap['kml'])
                linear_ring = etree.SubElement(outer_boundary, "{%s}LinearRing" % nsmap['kml'])
                coord_elem = etree.SubElement(linear_ring, "{%s}coordinates" % nsmap['kml'])
                coord_elem.text = coordinates
                logging.debug(f"Added Polygon geometry to Placemark '{row['name']}'")
            elif geometry_type == 'LineString':
                linestring = etree.SubElement(placemark, "{%s}LineString" % nsmap['kml'])
                coord_elem = etree.SubElement(linestring, "{%s}coordinates" % nsmap['kml'])
                coord_elem.text = coordinates
                logging.debug(f"Added LineString geometry to Placemark '{row['name']}'")
            else:
                point = etree.SubElement(placemark, "{%s}Point" % nsmap['kml'])
                coord_elem = etree.SubElement(point, "{%s}coordinates" % nsmap['kml'])
                coord_elem.text = coordinates
                logging.debug(f"Added Point geometry to Placemark '{row['name']}'")

        if ('longitude' in row and 'latitude' in row and
            is_valid_number(row['longitude']) and is_valid_number(row['latitude'])):
            lookat = etree.SubElement(placemark, "{%s}LookAt" % nsmap['kml'])
            etree.SubElement(lookat, "{%s}longitude" % nsmap['kml']).text = str(row['longitude'])
            etree.SubElement(lookat, "{%s}latitude" % nsmap['kml']).text = str(row['latitude'])
            etree.SubElement(lookat, "{%s}altitude" % nsmap['kml']).text = str(row['altitude']) if 'altitude' in row and row['altitude'] is not None else "0"
            etree.SubElement(lookat, "{%s}heading" % nsmap['kml']).text = str(row['heading']) if 'heading' in row and row['heading'] is not None else "0"
            etree.SubElement(lookat, "{%s}tilt" % nsmap['kml']).text = str(row['tilt']) if 'tilt' in row and row['tilt'] is not None else "0"
            etree.SubElement(lookat, "{%s}range" % nsmap['kml']).text = str(row['range']) if 'range' in row and row['range'] is not None else "0"
            if 'altitude_mode' in row and row['altitude_mode']:
                altitude_mode_elem = etree.SubElement(lookat, "{%s}altitudeMode" % nsmap['kml'])
                altitude_mode_elem.text = row['altitude_mode']
            logging.debug(f"Added LookAt to Placemark '{row['name']}'")

        date_acq = row['date_acq'] if 'date_acq' in row else None
        if date_acq:
            try:
                if '<begin>' in date_acq and '<end>' in date_acq:
                    begin_match = re.search(r'<begin>(.*?)</begin>', date_acq)
                    end_match = re.search(r'<end>(.*?)</end>', date_acq)
                    if begin_match and end_match:
                        begin_time = begin_match.group(1)
                        end_time = end_match.group(1)
                        timespan = etree.SubElement(placemark, "{%s}TimeSpan" % nsmap['kml'])
                        begin_elem = etree.SubElement(timespan, "{%s}begin" % nsmap['kml'])
                        begin_elem.text = begin_time
                        end_elem = etree.SubElement(timespan, "{%s}end" % nsmap['kml'])
                        end_elem.text = end_time
                        logging.debug(f"Added TimeSpan to Placemark '{row['name']}'")
                else:
                    date_obj = None
                    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                        try:
                            date_obj = datetime.strptime(date_acq, fmt)
                            break
                        except ValueError:
                            continue
                    if date_obj:
                        timestamp = etree.SubElement(placemark, "{%s}TimeStamp" % nsmap['kml'])
                        when = etree.SubElement(timestamp, "{%s}when" % nsmap['kml'])
                        when.text = date_obj.isoformat() + 'Z'
                        logging.debug(f"Added TimeStamp to Placemark '{row['name']}'")
                    else:
                        logging.warning(f"Failed to parse date_acq '{date_acq}' for Placemark '{row['name']}'")
            except Exception as e:
                logging.warning(f"Error processing date_acq '{date_acq}' for Placemark '{row['name']}': {e}")

        style = etree.SubElement(placemark, "{%s}Style" % nsmap['kml'])
        voltage = row['voltage'] if 'voltage' in row else None
        color = map_voltage_to_color(voltage)
        conductor_type = row['cable'] if 'cable' in row else None
        width = None
        if conductor_type:
            width = get_conductor_width(conn, conductor_type, row['cable'])

        linestyle = etree.SubElement(style, "{%s}LineStyle" % nsmap['kml'])
        line_color = etree.SubElement(linestyle, "{%s}color" % nsmap['kml'])
        line_color.text = color
        line_width = etree.SubElement(linestyle, "{%s}width" % nsmap['kml'])
        line_width.text = f"{width:.2f}" if width else "1"
        logging.debug(f"Set LineStyle for Placemark '{row['name']}'")

        if 'poly_color' in row or 'poly_opacity' in row:
            polystyle = etree.SubElement(style, "{%s}PolyStyle" % nsmap['kml'])
            if 'poly_color' in row and row['poly_color']:
                poly_color = etree.SubElement(polystyle, "{%s}color" % nsmap['kml'])
                poly_color.text = row['poly_color']
            if 'poly_opacity' in row and row['poly_opacity']:
                poly_opacity = etree.SubElement(polystyle, "{%s}opacity" % nsmap['kml'])
                poly_opacity.text = str(row['poly_opacity'])

        if 'icon_href' in row or 'icon_scale' in row or 'icon_color' in row:
            iconstyle = etree.SubElement(style, "{%s}IconStyle" % nsmap['kml'])
            if 'icon_scale' in row and row['icon_scale']:
                icon_scale = etree.SubElement(iconstyle, "{%s}scale" % nsmap['kml'])
                icon_scale.text = str(row['icon_scale'])
            if 'icon_color' in row and row['icon_color']:
                icon_color = etree.SubElement(iconstyle, "{%s}color" % nsmap['kml'])
                icon_color.text = row['icon_color']
            icon = etree.SubElement(iconstyle, "{%s}Icon" % nsmap['kml'])
            href = etree.SubElement(icon, "{%s}href" % nsmap['kml'])
            href.text = row['icon_href'] if 'icon_href' in row and row['icon_href'] else ""

        if 'label_color' in row or 'label_scale' in row:
            labelstyle = etree.SubElement(style, "{%s}LabelStyle" % nsmap['kml'])
            if 'label_color' in row and row['label_color']:
                label_color = etree.SubElement(labelstyle, "{%s}color" % nsmap['kml'])
                label_color.text = row['label_color']
            if 'label_scale' in row and row['label_scale']:
                label_scale = etree.SubElement(labelstyle, "{%s}scale" % nsmap['kml'])
                label_scale.text = str(row['label_scale'])

        if 'extended_data' in row and row['extended_data']:
            add_extended_data(placemark, row['extended_data'], nsmap)

    placemark_objects = []
    for row in placemarks:
        if row['geometry_type'] in ['LineString', 'MultiGeometry']:
            placemark_obj = Placemark(row)
            placemark_objects.append(placemark_obj)

    print("Total placemark_objects:", len(placemark_objects))

    if find_pairs:
        grid_index = build_spatial_index_with_names(placemark_objects, GRID_SIZE_DEGREES)
        identified_pairs = find_identified_pairs(grid_index, proximity_threshold, angle_threshold)
        print("Total identified_pairs:", len(identified_pairs))
        with open('outputs/lines_in_same_row.txt', 'w') as f:
            for name1, name2, seg1, seg2, distance, angle_diff in identified_pairs:
                f.write(f"{name1} and {name2} share the same ROW\n")
                f.write(f"Segment from {name1}: {seg1}\n")
                f.write(f"Segment from {name2}: {seg2}\n")
                f.write(f"Distance between segments: {distance:.2f} meters\n")
                f.write(f"Angle difference: {angle_diff:.2f} degrees\n\n")
        plot_grids_and_lines(grid_index, identified_pairs, output_plot='outputs/grid_plot.pdf')
    else:
        logging.info("Skipping pair finding as per user request.")

    for row in groundoverlays:
        folder_hierarchy = row['folder_hierarchy']
        folder_elem = get_folder_element(folder_hierarchy, document, folder_dict, nsmap) if folder_hierarchy else document

        groundoverlay = etree.SubElement(folder_elem, "{%s}GroundOverlay" % nsmap['kml'])
        name_elem = etree.SubElement(groundoverlay, "{%s}name" % nsmap['kml'])
        name_elem.text = row['name'] if row['name'] else "Unnamed GroundOverlay"

        icon = etree.SubElement(groundoverlay, "{%s}Icon" % nsmap['kml'])
        href = etree.SubElement(icon, "{%s}href" % nsmap['kml'])
        new_icon_href = sanitize_icon_href_for_groundoverlays(row['icon_href']) if 'icon_href' in row else ""
        href.text = new_icon_href

        if 'view_bound_scale' in row and row['view_bound_scale'] is not None:
            view_bound_scale_elem = etree.SubElement(icon, "{%s}viewBoundScale" % nsmap['kml'])
            view_bound_scale_elem.text = str(row['view_bound_scale'])

        if 'coordinates' in row and row['coordinates']:
            latlonquad = etree.SubElement(groundoverlay, "{%s}LatLonQuad" % nsmap['gx'])
            coord_elem = etree.SubElement(latlonquad, "{%s}coordinates" % nsmap['kml'])
            coord_elem.text = row['coordinates']
        else:
            latlonbox = etree.SubElement(groundoverlay, "{%s}LatLonBox" % nsmap['kml'])
            etree.SubElement(latlonbox, "{%s}north" % nsmap['kml']).text = str(row['north']) if row['north'] is not None else "0"
            etree.SubElement(latlonbox, "{%s}south" % nsmap['kml']).text = str(row['south']) if row['south'] is not None else "0"
            etree.SubElement(latlonbox, "{%s}east" % nsmap['kml']).text = str(row['east']) if row['east'] is not None else "0"
            etree.SubElement(latlonbox, "{%s}west" % nsmap['kml']).text = str(row['west']) if row['west'] is not None else "0"
            if 'rotation' in row and row['rotation'] is not None:
                rotation_elem = etree.SubElement(latlonbox, "{%s}rotation" % nsmap['kml'])
                rotation_elem.text = str(row['rotation'])

        if ('longitude' in row and 'latitude' in row and
            is_valid_number(row['longitude']) and is_valid_number(row['latitude'])):
            lookat = etree.SubElement(groundoverlay, "{%s}LookAt" % nsmap['kml'])
            etree.SubElement(lookat, "{%s}longitude" % nsmap['kml']).text = str(row['longitude'])
            etree.SubElement(lookat, "{%s}latitude" % nsmap['kml']).text = str(row['latitude'])
            etree.SubElement(lookat, "{%s}altitude" % nsmap['kml']).text = str(row['altitude']) if row['altitude'] is not None else "0"
            etree.SubElement(lookat, "{%s}heading" % nsmap['kml']).text = str(row['heading']) if row['heading'] is not None else "0"
            etree.SubElement(lookat, "{%s}tilt" % nsmap['kml']).text = str(row['tilt']) if row['tilt'] is not None else "0"
            etree.SubElement(lookat, "{%s}range" % nsmap['kml']).text = str(row['range']) if row['range'] is not None else "0"
            if 'altitude_mode' in row and row['altitude_mode']:
                altitude_mode_elem = etree.SubElement(lookat, "{%s}altitudeMode" % nsmap['kml'])
                altitude_mode_elem.text = row['altitude_mode']

        date_acq = row['date_acq'] if 'date_acq' in row else None
        if date_acq:
            try:
                date_obj = None
                for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"):
                    try:
                        date_obj = datetime.strptime(date_acq, fmt)
                        break
                    except ValueError:
                        continue
                if date_obj:
                    timestamp = etree.SubElement(groundoverlay, "{%s}TimeStamp" % nsmap['kml'])
                    when = etree.SubElement(timestamp, "{%s}when" % nsmap['kml'])
                    when.text = date_obj.isoformat()
            except Exception as e:
                logging.warning(f"Failed to parse date_acq '{date_acq}' for GroundOverlay '{row['name']}': {e}")

        if 'extended_data' in row and row['extended_data']:
            add_extended_data(groundoverlay, row['extended_data'], nsmap)

    for row in networklinks:
        folder_hierarchy = row['folder_hierarchy']
        folder_elem = get_folder_element(folder_hierarchy, document, folder_dict, nsmap) if folder_hierarchy else document

        networklink = etree.SubElement(folder_elem, "{%s}NetworkLink" % nsmap['kml'])

        name_elem = etree.SubElement(networklink, "{%s}name" % nsmap['kml'])
        name_elem.text = row['name'] if row['name'] else "Unnamed NetworkLink"

        visibility_elem = etree.SubElement(networklink, "{%s}visibility" % nsmap['kml'])
        visibility_elem.text = str(row['visibility']) if 'visibility' in row and row['visibility'] is not None else "1"

        if ('longitude' in row and 'latitude' in row and
            is_valid_number(row['longitude']) and is_valid_number(row['latitude'])):
            lookat = etree.SubElement(networklink, "{%s}LookAt" % nsmap['kml'])
            etree.SubElement(lookat, "{%s}longitude" % nsmap['kml']).text = str(row['longitude'])
            etree.SubElement(lookat, "{%s}latitude" % nsmap['kml']).text = str(row['latitude'])
            etree.SubElement(lookat, "{%s}altitude" % nsmap['kml']).text = str(row['altitude']) if row['altitude'] is not None else "0"
            etree.SubElement(lookat, "{%s}heading" % nsmap['kml']).text = str(row['heading']) if row['heading'] is not None else "0"
            etree.SubElement(lookat, "{%s}tilt" % nsmap['kml']).text = str(row['tilt']) if row['tilt'] is not None else "0"
            etree.SubElement(lookat, "{%s}range" % nsmap['kml']).text = str(row['range']) if row['range'] is not None else "0"
            if 'altitude_mode' in row and row['altitude_mode']:
                altitude_mode_elem = etree.SubElement(lookat, "{%s}altitudeMode" % nsmap['kml'])
                altitude_mode_elem.text = row['altitude_mode']

        date_acq = row['date_acq'] if 'date_acq' in row else None
        if date_acq:
            try:
                date_obj = None
                for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"):
                    try:
                        date_obj = datetime.strptime(date_acq, fmt)
                        break
                    except ValueError:
                        continue
                if date_obj:
                    timestamp = etree.SubElement(networklink, "{%s}TimeStamp" % nsmap['kml'])
                    when = etree.SubElement(timestamp, "{%s}when" % nsmap['kml'])
                    when.text = date_obj.isoformat()
            except Exception as e:
                logging.warning(f"Failed to parse date_acq '{date_acq}' for NetworkLink '{row['name']}': {e}")

        url = etree.SubElement(networklink, "{%s}Url" % nsmap['kml'])
        href = etree.SubElement(url, "{%s}href" % nsmap['kml'])
        href.text = row['href'] if 'href' in row and row['href'] else ""
        view_refresh_mode = etree.SubElement(url, "{%s}viewRefreshMode" % nsmap['kml'])
        view_refresh_mode.text = row['viewRefreshMode'] if 'viewRefreshMode' in row and row['viewRefreshMode'] else ""
        view_refresh_time = etree.SubElement(url, "{%s}viewRefreshTime" % nsmap['kml'])
        view_refresh_time.text = str(row['viewRefreshTime']) if 'viewRefreshTime' in row and row['viewRefreshTime'] is not None else "0"

        if 'extended_data' in row and row['extended_data']:
            add_extended_data(networklink, row['extended_data'], nsmap)

    conn.close()
    logging.info("KML reconstruction completed.")

    return kml_root, document

def create_kmz(kml_file, kmz_file, source_folder):
    logging.info("Starting KMZ creation...")
    try:
        with zipfile.ZipFile(kmz_file, 'w', zipfile.ZIP_DEFLATED) as kmz:
            kmz.write(kml_file, os.path.basename(kml_file))
            kml_size = os.path.getsize(kml_file)
            logging.debug(f"Added KML file to KMZ: {kml_file} (Size: {kml_size} bytes)")

            if os.path.isdir(source_folder):
                for root_dir, dirs, files in os.walk(source_folder):
                    for file in files:
                        file_path = os.path.join(root_dir, file)
                        arcname = os.path.relpath(file_path, source_folder)
                        _, ext = os.path.splitext(file)
                        ext = ext.lower()
                        if ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.svg']:
                            compressed_image_data, image_format = compress_image(file_path)
                            if compressed_image_data:
                                kmz.writestr(arcname, compressed_image_data)
                                compressed_size = len(compressed_image_data)
                                logging.debug(f"Compressed and added image to KMZ: {file_path} as {arcname} (Size: {compressed_size} bytes)")
                            else:
                                kmz.write(file_path, arcname)
                                original_size = os.path.getsize(file_path)
                                logging.debug(f"Added original image to KMZ: {file_path} as {arcname} (Size: {original_size} bytes)")
                        else:
                            kmz.write(file_path, arcname)
                            file_size = os.path.getsize(file_path)
                            logging.debug(f"Added non-image file to KMZ: {file_path} as {arcname} (Size: {file_size} bytes)")
            else:
                logging.warning(f"Source folder for KMZ resources does not exist: {source_folder}")

        final_kmz_size = os.path.getsize(kmz_file)
        logging.info(f"KMZ file successfully created at: {kmz_file} (Total Size: {final_kmz_size} bytes)")
    except Exception as e:
        logging.error(f"Failed to create KMZ file: {e}")

def add_svg_overlay(document, image_path, north, south, east, west, rotation=0):
    nsmap = document.nsmap
    groundoverlay = etree.SubElement(document, "{%s}GroundOverlay" % nsmap['kml'])
    name = etree.SubElement(groundoverlay, "{%s}name" % nsmap['kml'])
    name.text = "Image Overlay"
    icon = etree.SubElement(groundoverlay, "{%s}Icon" % nsmap['kml'])
    href = etree.SubElement(icon, "{%s}href" % nsmap['kml'])
    href.text = image_path

    latlonbox = etree.SubElement(groundoverlay, "{%s}LatLonBox" % nsmap['kml'])
    etree.SubElement(latlonbox, "{%s}north" % nsmap['kml']).text = str(north)
    etree.SubElement(latlonbox, "{%s}south" % nsmap['kml']).text = str(south)
    etree.SubElement(latlonbox, "{%s}east" % nsmap['kml']).text = str(east)
    etree.SubElement(latlonbox, "{%s}west" % nsmap['kml']).text = str(west)
    if rotation:
        etree.SubElement(latlonbox, "{%s}rotation" % nsmap['kml']).text = str(rotation)
    logging.info("Added image GroundOverlay with bounding box coordinates.")

def reconstruct_kml_from_db(db_path, output_kml, find_pairs=True):
    return reconstruct_kml(db_path, output_kml, find_pairs=find_pairs)

def main():
    parser = argparse.ArgumentParser(description='Reconstruct KML and create KMZ.')
    parser.add_argument('--find-pairs', action='store_true', help='Find and process line pairs')
    args = parser.parse_args()

    find_pairs = args.find_pairs

    current_dir = os.getcwd()
    db_path = os.path.join(current_dir, 'outputs', 'kmz.db')
    output_kml = os.path.join(current_dir, 'outputs', 'reconstructed.kml')
    files_folder = os.path.join(current_dir, 'outputs', 'files')
    kmz_file = os.path.join(current_dir, 'outputs', 'reconstructed.kmz')

    os.makedirs(os.path.dirname(output_kml), exist_ok=True)
    os.makedirs(files_folder, exist_ok=True)

    kml_root, document = reconstruct_kml_from_db(db_path, output_kml, find_pairs=find_pairs)
    add_svg_overlay(document, "files/station_diagram.png", north=30.0, south=29.9, east=-95.0, west=-95.1)

    tree = etree.ElementTree(kml_root)
    try:
        tree.write(output_kml, pretty_print=True, xml_declaration=True, encoding='UTF-8')
        logging.info(f"KML file successfully created at: {output_kml}")
    except Exception as e:
        logging.error(f"Failed to write KML file: {e}")

    create_kmz(output_kml, kmz_file, files_folder)
    logging.info("Script execution completed successfully.")

if __name__ == "__main__":
    main()
