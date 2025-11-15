"""Utilities for building geography translation maps.

Currently supports deriving a mapping from census tracts to the ZIP code
containing the largest share of each tract's area.  The implementation avoids
geospatial dependencies (e.g., shapely/geopandas) so that the repository can be
used in restricted environments.  Instead, it performs a fine grid sampling of
each tract, weights the samples by the cosine of the latitude to approximate an
equal-area projection, and assigns each sample to the ZIP polygon that contains
it.  The ZIP with the greatest weighted share is selected for each tract.

This module intentionally trades a small amount of precision for portability.
The sampling resolution is high enough to provide stable results for the
Chicago-area datasets contained in ``src/data/spatial``.
"""

from __future__ import annotations

import csv
import json
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple


Point = Tuple[float, float]


def _ensure_closed(coords: Sequence[Sequence[float]]) -> List[Point]:
    """Return a closed ring from the provided coordinates."""

    if not coords:
        return []
    ring: List[Point] = [(float(x), float(y)) for x, y in coords]
    if ring[0] != ring[-1]:
        ring.append(ring[0])
    return ring


def _point_on_segment(point: Point, start: Point, end: Point, *, eps: float = 1e-12) -> bool:
    """Return True when *point* is on the segment from *start* to *end*."""

    px, py = point
    x1, y1 = start
    x2, y2 = end
    cross = (x2 - x1) * (py - y1) - (y2 - y1) * (px - x1)
    if abs(cross) > eps:
        return False
    dot = (px - x1) * (px - x2) + (py - y1) * (py - y2)
    return dot <= eps


def _point_in_ring(point: Point, ring: Sequence[Point]) -> bool:
    """Ray-casting point-in-polygon test for a closed ring."""

    if len(ring) < 4:  # minimum for closed triangle
        return False
    px, py = point
    inside = False
    for i in range(len(ring) - 1):
        x1, y1 = ring[i]
        x2, y2 = ring[i + 1]
        if _point_on_segment(point, (x1, y1), (x2, y2)):
            return True
        if ((y1 > py) != (y2 > py)) and (y2 != y1):
            xinters = (x2 - x1) * (py - y1) / (y2 - y1) + x1
            if abs(xinters - px) < 1e-12:
                return True
            if xinters > px:
                inside = not inside
    return inside


def _ring_centroid_and_area(ring: Sequence[Point]) -> Tuple[Point, float]:
    """Return (centroid, signed area) for a closed ring."""

    if len(ring) < 4:
        return ((ring[0][0], ring[0][1]) if ring else (0.0, 0.0), 0.0)
    twice_area = 0.0
    cx = 0.0
    cy = 0.0
    for i in range(len(ring) - 1):
        x1, y1 = ring[i]
        x2, y2 = ring[i + 1]
        cross = x1 * y2 - x2 * y1
        twice_area += cross
        cx += (x1 + x2) * cross
        cy += (y1 + y2) * cross
    area = twice_area / 2.0
    if abs(area) < 1e-12:
        return (ring[0], 0.0)
    cx /= (3.0 * twice_area)
    cy /= (3.0 * twice_area)
    return (cx, cy), area


@dataclass
class Polygon:
    exterior: List[Point]
    holes: List[List[Point]]

    def __post_init__(self) -> None:
        xs = [x for x, _ in self.exterior]
        ys = [y for _, y in self.exterior]
        self.minx = min(xs)
        self.maxx = max(xs)
        self.miny = min(ys)
        self.maxy = max(ys)
        centroid, area = _ring_centroid_and_area(self.exterior)
        total_area = area
        cx, cy = centroid
        for hole in self.holes:
            hole_centroid, hole_area = _ring_centroid_and_area(hole)
            total_area += hole_area
            cx += hole_centroid[0] * hole_area
            cy += hole_centroid[1] * hole_area
        self._signed_area = total_area
        if abs(total_area) < 1e-12:
            self._centroid = self.exterior[0]
        else:
            self._centroid = (cx / total_area, cy / total_area)

    def contains(self, point: Point) -> bool:
        if not _point_in_ring(point, self.exterior):
            return False
        for hole in self.holes:
            if _point_in_ring(point, hole):
                return False
        return True

    def centroid(self) -> Point:
        return self._centroid

    def area(self) -> float:
        return self._signed_area


@dataclass
class MultiPolygon:
    polygons: List[Polygon]

    def __post_init__(self) -> None:
        self.minx = min(p.minx for p in self.polygons)
        self.maxx = max(p.maxx for p in self.polygons)
        self.miny = min(p.miny for p in self.polygons)
        self.maxy = max(p.maxy for p in self.polygons)

    def contains(self, point: Point) -> bool:
        return any(p.contains(point) for p in self.polygons)

    def centroid(self) -> Point:
        total_area = 0.0
        cx = 0.0
        cy = 0.0
        for polygon in self.polygons:
            area = polygon.area()
            if abs(area) < 1e-12:
                continue
            poly_centroid = polygon.centroid()
            total_area += area
            cx += poly_centroid[0] * area
            cy += poly_centroid[1] * area
        if abs(total_area) < 1e-12:
            return self.polygons[0].centroid()
        return (cx / total_area, cy / total_area)


def _load_geometry(geometry: dict) -> MultiPolygon:
    if geometry["type"] == "Polygon":
        coords = geometry["coordinates"]
        polygon = Polygon(
            exterior=_ensure_closed(coords[0]),
            holes=[_ensure_closed(ring) for ring in coords[1:]],
        )
        return MultiPolygon([polygon])
    elif geometry["type"] == "MultiPolygon":
        polygons = []
        for poly_coords in geometry["coordinates"]:
            polygon = Polygon(
                exterior=_ensure_closed(poly_coords[0]),
                holes=[_ensure_closed(ring) for ring in poly_coords[1:]],
            )
            polygons.append(polygon)
        return MultiPolygon(polygons)
    else:
        raise ValueError(f"Unsupported geometry type: {geometry['type']}")


@dataclass
class SpatialFeature:
    identifier: str
    geometry: MultiPolygon

    @property
    def bbox(self) -> Tuple[float, float, float, float]:
        return (self.geometry.minx, self.geometry.miny, self.geometry.maxx, self.geometry.maxy)

    def contains(self, point: Point) -> bool:
        x, y = point
        minx, miny, maxx, maxy = self.bbox
        if x < minx or x > maxx or y < miny or y > maxy:
            return False
        return self.geometry.contains(point)

    def centroid(self) -> Point:
        return self.geometry.centroid()


def _bbox_overlaps(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float]) -> bool:
    minx1, miny1, maxx1, maxy1 = a
    minx2, miny2, maxx2, maxy2 = b
    return not (maxx1 < minx2 or maxx2 < minx1 or maxy1 < miny2 or maxy2 < miny1)


def _sample_points_within(feature: SpatialFeature, target: int = 80) -> List[Point]:
    rng = random.Random(feature.identifier)
    minx, miny, maxx, maxy = feature.bbox
    width = maxx - minx
    height = maxy - miny
    if width <= 0 and height <= 0:
        return [feature.centroid()]
    if width <= 0:
        width = 1e-9
    if height <= 0:
        height = 1e-9
    points: List[Point] = []
    attempts = 0
    max_attempts = target * 50
    while len(points) < target and attempts < max_attempts:
        x = rng.uniform(minx, maxx)
        y = rng.uniform(miny, maxy)
        attempts += 1
        if feature.contains((x, y)):
            points.append((x, y))
    if not points:
        points.append(feature.centroid())
    return points


def load_features(path: Path, identifier_field: str) -> List[SpatialFeature]:
    with path.open() as f:
        data = json.load(f)
    features = []
    for feature in data["features"]:
        identifier = str(feature["properties"][identifier_field])
        geometry = _load_geometry(feature["geometry"])
        features.append(SpatialFeature(identifier=identifier, geometry=geometry))
    return features


def build_census_tract_to_zip_map(
    tracts: List[SpatialFeature], zips: List[SpatialFeature]
) -> List[Tuple[str, Optional[str]]]:
    mapping: List[Tuple[str, Optional[str]]] = []
    zip_bboxes = [zip_feat.bbox for zip_feat in zips]

    for tract in tracts:
        tract_bbox = tract.bbox
        candidate_indices = [
            idx for idx, bbox in enumerate(zip_bboxes) if _bbox_overlaps(tract_bbox, bbox)
        ]
        candidate_zips = [zips[idx] for idx in candidate_indices]

        counts: dict[str, float] = {}
        for point in _sample_points_within(tract):
            weight = math.cos(math.radians(point[1]))
            assigned: Optional[str] = None
            for zip_feature in candidate_zips:
                if zip_feature.contains(point):
                    assigned = zip_feature.identifier
                    break
            if assigned is not None:
                counts[assigned] = counts.get(assigned, 0.0) + weight
        selected_zip: Optional[str]
        if counts:
            selected_zip = max(counts.items(), key=lambda kv: (kv[1], kv[0]))[0]
        else:
            centroid = tract.centroid()
            selected_zip = None
            for zip_feature in candidate_zips:
                if zip_feature.contains(centroid):
                    selected_zip = zip_feature.identifier
                    break
            if selected_zip is None and candidate_zips:
                cx, cy = centroid
                best_dist = float("inf")
                for zip_feature in candidate_zips:
                    zx, zy = zip_feature.centroid()
                    dist = (zx - cx) ** 2 + (zy - cy) ** 2
                    if dist < best_dist:
                        best_dist = dist
                        selected_zip = zip_feature.identifier
        mapping.append((tract.identifier, selected_zip))
    return mapping


def write_mapping_csv(
    mapping: Sequence[Tuple[str, Optional[str]]], path: Path, *, header: Tuple[str, str]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for key, value in mapping:
            writer.writerow([key, value if value is not None else ""])


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    spatial_dir = repo_root / "data" / "spatial"
    tracts = load_features(spatial_dir / "census_tracts.geojson", "census_t_1")
    zips = load_features(spatial_dir / "zip_codes.geojson", "zip")
    mapping = build_census_tract_to_zip_map(tracts, zips)
    output_path = repo_root / "data" / "census_tract_to_zip_code.csv"
    write_mapping_csv(mapping, output_path, header=("census_tract", "zip_code"))


if __name__ == "__main__":
    main()