from xml.etree import ElementTree as ET
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, Circle, Polygon, PathPatch
from matplotlib.path import Path
import numpy as np
import os

# Function to parse the SVG file
def parse_svg_elements(svg_file):
    tree = ET.parse(svg_file)
    root = tree.getroot()

    # Namespace for SVG files
    namespace = {'svg': 'http://www.w3.org/2000/svg'}
    elements_info = []

    # Extract rectangles
    for rect in root.findall(".//svg:rect", namespace):
        x = float(rect.attrib.get('x', '0'))
        y = float(rect.attrib.get('y', '0'))
        width = float(rect.attrib.get('width', '0'))
        height = float(rect.attrib.get('height', '0'))
        fill = rect.attrib.get('fill', 'none')
        stroke = rect.attrib.get('stroke', 'none')
        stroke_width = float(rect.attrib.get('stroke-width', '1'))
        opacity = float(rect.attrib.get('opacity', '1'))
        elements_info.append({
            'type': 'rectangle',
            'x': x, 'y': y, 'width': width, 'height': height,
            'fill': fill, 'stroke': stroke, 'stroke_width': stroke_width,
            'opacity': opacity
        })

    # Extract circles
    for circle in root.findall(".//svg:circle", namespace):
        cx = float(circle.attrib.get('cx', '0'))
        cy = float(circle.attrib.get('cy', '0'))
        r = float(circle.attrib.get('r', '0'))
        fill = circle.attrib.get('fill', 'none')
        stroke = circle.attrib.get('stroke', 'none')
        stroke_width = float(circle.attrib.get('stroke-width', '1'))
        elements_info.append({
            'type': 'circle',
            'cx': cx, 'cy': cy, 'r': r, 
            'fill': fill, 'stroke': stroke, 'stroke_width': stroke_width
        })

    # Extract lines
    for line in root.findall(".//svg:line", namespace):
        x1 = float(line.attrib.get('x1', '0'))
        y1 = float(line.attrib.get('y1', '0'))
        x2 = float(line.attrib.get('x2', '0'))
        y2 = float(line.attrib.get('y2', '0'))
        stroke = line.attrib.get('stroke', 'none')
        stroke_width = float(line.attrib.get('stroke-width', '1'))
        elements_info.append({
            'type': 'line',
            'x1': x1, 'y1': y1, 'x2': x2, 'y2': y2, 
            'stroke': stroke, 'stroke_width': stroke_width
        })

    # Extract polylines
    for polyline in root.findall(".//svg:polyline", namespace):
        points = polyline.attrib.get('points', '').strip()
        stroke = polyline.attrib.get('stroke', 'none')
        stroke_width = float(polyline.attrib.get('stroke-width', '1'))
        elements_info.append({
            'type': 'polyline',
            'points': points, 'stroke': stroke, 'stroke_width': stroke_width
        })

    # Extract paths
    for path in root.findall(".//svg:path", namespace):
        d = path.attrib.get('d', '')
        stroke = path.attrib.get('stroke', 'none')
        stroke_width = float(path.attrib.get('stroke-width', '1'))
        fill = path.attrib.get('fill', 'none')
        elements_info.append({
            'type': 'path',
            'd': d,
            'stroke': stroke,
            'stroke_width': stroke_width,
            'fill': fill
        })

    return elements_info

# Function to approximate an arc with line segments
def approximate_arc(rx, ry, rotation, large_arc, sweep, start, end, num_segments=50):
    """
    Approximate an elliptical arc using line segments.
    """
    dx = (start[0] - end[0]) / 2.0
    dy = (start[1] - end[1]) / 2.0
    cos_angle = np.cos(np.radians(rotation))
    sin_angle = np.sin(np.radians(rotation))

    x1_prime = cos_angle * dx + sin_angle * dy
    y1_prime = -sin_angle * dx + cos_angle * dy

    rx_sq = rx**2
    ry_sq = ry**2
    x1_prime_sq = x1_prime**2
    y1_prime_sq = y1_prime**2

    radii_check = x1_prime_sq / rx_sq + y1_prime_sq / ry_sq
    if radii_check > 1:
        scale = np.sqrt(radii_check)
        rx *= scale
        ry *= scale
        rx_sq = rx**2
        ry_sq = ry**2

    factor = np.sqrt(
        max(0, (rx_sq * ry_sq - rx_sq * y1_prime_sq - ry_sq * x1_prime_sq) /
                 (rx_sq * y1_prime_sq + ry_sq * x1_prime_sq))
    )
    if large_arc == sweep:
        factor = -factor

    cx_prime = factor * (rx * y1_prime / ry)
    cy_prime = factor * (-ry * x1_prime / rx)

    cx = cos_angle * cx_prime - sin_angle * cy_prime + (start[0] + end[0]) / 2
    cy = sin_angle * cx_prime + cos_angle * cy_prime + (start[1] + end[1]) / 2

    start_vector = ((x1_prime - cx_prime) / rx, (y1_prime - cy_prime) / ry)
    end_vector = ((-x1_prime - cx_prime) / rx, (-y1_prime - cy_prime) / ry)
    start_angle = np.arctan2(start_vector[1], start_vector[0])
    end_angle = np.arctan2(end_vector[1], end_vector[0])

    if not sweep and end_angle > start_angle:
        end_angle -= 2 * np.pi
    elif sweep and end_angle < start_angle:
        end_angle += 2 * np.pi

    angles = np.linspace(start_angle, end_angle, num_segments)
    x = cx + rx * np.cos(angles)
    y = cy + ry * np.sin(angles)

    return list(zip(x, y))

# Function to parse paths, including arcs
def parse_svg_path(d):
    import re
    command_re = re.compile(r'([MLAZ])|(-?\d+(\.\d+)?)')
    commands = command_re.findall(d)
    commands = [cmd[0] or cmd[1] for cmd in commands]

    vertices = []
    codes = []
    idx = 0

    while idx < len(commands):
        cmd = commands[idx]
        idx += 1
        print("cmd:", cmd)

        if cmd == 'M':  # Move to
            x, y = float(commands[idx]), float(commands[idx + 1])
            idx += 2
            vertices.append((x, y))
            codes.append(Path.MOVETO)

        elif cmd == 'L':  # Line to
            x, y = float(commands[idx]), float(commands[idx + 1])
            idx += 2
            vertices.append((x, y))
            codes.append(Path.LINETO)

        elif cmd == 'A' or cmd == "a":  # Arc
            print("found arc")
            rx = float(commands[idx])
            ry = float(commands[idx + 1])
            rotation = float(commands[idx + 2])
            large_arc = int(commands[idx + 3])
            sweep = int(commands[idx + 4])
            x, y = float(commands[idx + 5]), float(commands[idx + 6])
            idx += 7
            start = vertices[-1] if vertices else (0, 0)
            arc_vertices = approximate_arc(rx, ry, rotation, large_arc, sweep, start, (x, y))
            vertices.extend(arc_vertices)
            codes.extend([Path.LINETO] * len(arc_vertices))

        elif cmd == 'Z':  # Close path
            vertices.append(vertices[0])
            codes.append(Path.CLOSEPOLY)

    return vertices, codes

# Function to plot SVG elements
def plot_svg_elements(elements):
    fig, ax = plt.subplots()
    ax.set_aspect('equal')
    ax.set_title('SVG Visualization')
    ax.set_xlabel('X')
    ax.set_ylabel('Y')

    for element in elements:
        if element['type'] == 'rectangle':
            rect = Rectangle(
                (element['x'], element['y']), 
                element['width'], element['height'], 
                edgecolor=element['stroke'], 
                facecolor=element['fill'],
                alpha=element['opacity'], 
                linewidth=element['stroke_width'])
            ax.add_patch(rect)
        elif element['type'] == 'circle':
            circle = Circle(
                (element['cx'], element['cy']), element['r'], 
                edgecolor=element['stroke'], facecolor=element['fill'], 
                linewidth=element['stroke_width'], fill=True)
            ax.add_artist(circle)
        elif element['type'] == 'line':
            ax.plot(
                [element['x1'], element['x2']], 
                [element['y1'], element['y2']], 
                color=element['stroke'], linewidth=element['stroke_width'])
        elif element['type'] == 'polyline':
            points = [tuple(map(float, point.split(','))) 
                      for point in element['points'].split()]
            polygon = Polygon(points, closed=False, edgecolor=element['stroke'], fill=False)
            ax.add_patch(polygon)
        elif element['type'] == 'path':
            vertices, codes = parse_svg_path(element['d'])
            if vertices and codes:
                path = Path(vertices, codes)
                patch = PathPatch(path, edgecolor=element['stroke'], linewidth=element['stroke_width'], fill=False)
                ax.add_patch(patch)

    plt.gca().invert_yaxis()
    plt.show()

# File path for the SVG
svg_file_path = 'data/SVGOverlay_Example.svg'

if os.path.exists(svg_file_path):
    svg_elements_info = parse_svg_elements(svg_file_path)
    plot_svg_elements(svg_elements_info)
else:
    print(f"File not found: {svg_file_path}")
