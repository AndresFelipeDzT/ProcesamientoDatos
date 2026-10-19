"""
Módulo del Modelo de RDD (Resilient Distributed Dataset).
Implementa la estructura central de Spark: inmutabilidad, particiones,
tipos de dependencia (Narrow/Wide) y rastreo de linaje (Lineage DAG).
"""

from typing import List, Optional, Dict, Any
from config import DependencyType
from models.partition import Partition

class RDD:
    """
    Representa un Resilient Distributed Dataset.
    Es inmutable, particionado y mantiene un puntero a sus padres (Lineage).
    """
    _id_counter = 0

    def __init__(
        self,
        name: str,
        operation: str,
        num_partitions: int,
        parents: Optional[List["RDD"]] = None,
        dependency_type: str = DependencyType.NARROW,
        total_data_size_mb: float = 100.0,
        skew_factor: float = 1.0  # 1.0 = balanceado, >1.0 = desbalanceado (skew)
    ):
        RDD._id_counter += 1
        self.id: int = RDD._id_counter
        self.name: str = name
        self.operation: str = operation
        self.num_partitions: int = num_partitions
        self.parents: List["RDD"] = parents if parents is not None else []
        self.dependency_type: str = dependency_type
        self.is_cached: bool = False
        self.storage_level: str = "NONE"
        self.total_data_size_mb: float = total_data_size_mb

        # Crear particiones iniciales con cálculo de tamaño y posible sesgo (skew)
        self.partitions: List[Partition] = self._create_partitions(skew_factor)

    def _create_partitions(self, skew_factor: float) -> List[Partition]:
        """Crea las particiones distribuyendo los MB totales."""
        parts = []
        base_size = self.total_data_size_mb / max(1, self.num_partitions)
        for i in range(self.num_partitions):
            # Si hay skew_factor, la partición 0 o la última se lleva más datos
            size = base_size
            if skew_factor > 1.0:
                if i == 0:
                    size = base_size * skew_factor
                else:
                    size = max(1.0, (self.total_data_size_mb - (base_size * skew_factor)) / max(1, self.num_partitions - 1))
            parts.append(Partition(index=i, rdd_id=self.id, size_mb=size))
        return parts

    # ==========================================
    # Transformaciones (Construyen el DAG)
    # ==========================================

    def map(self, name: str = "map()") -> "RDD":
        """Transformación Narrow: 1-a-1 sin Shuffle."""
        return RDD(
            name=name,
            operation="map",
            num_partitions=self.num_partitions,
            parents=[self],
            dependency_type=DependencyType.NARROW,
            total_data_size_mb=self.total_data_size_mb
        )

    def filter(self, name: str = "filter()") -> "RDD":
        """Transformación Narrow: reduce tamaño ligeramente sin shuffle."""
        return RDD(
            name=name,
            operation="filter",
            num_partitions=self.num_partitions,
            parents=[self],
            dependency_type=DependencyType.NARROW,
            total_data_size_mb=self.total_data_size_mb * 0.7
        )

    def group_by_key(self, name: str = "groupByKey()", num_partitions: Optional[int] = None, skew_factor: float = 1.0) -> "RDD":
        """Transformación Wide: genera límite de Stage y requiere Shuffle en red."""
        parts = num_partitions if num_partitions is not None else self.num_partitions
        return RDD(
            name=name,
            operation="groupByKey",
            num_partitions=parts,
            parents=[self],
            dependency_type=DependencyType.WIDE,
            total_data_size_mb=self.total_data_size_mb,
            skew_factor=skew_factor
        )

    def reduce_by_key(self, name: str = "reduceByKey()", num_partitions: Optional[int] = None) -> "RDD":
        """Transformación Wide con combinación local previa (Combiner/Shuffle)."""
        parts = num_partitions if num_partitions is not None else self.num_partitions
        return RDD(
            name=name,
            operation="reduceByKey",
            num_partitions=parts,
            parents=[self],
            dependency_type=DependencyType.WIDE,
            total_data_size_mb=self.total_data_size_mb * 0.5
        )

    # ==========================================
    # Caché y Persistencia
    # ==========================================

    def persist(self, storage_level: str = "MEMORY_ONLY") -> "RDD":
        """Marca el RDD para ser almacenado en RAM al calcularse."""
        self.is_cached = True
        self.storage_level = storage_level
        return self

    def cache(self) -> "RDD":
        return self.persist("MEMORY_ONLY")

    # ==========================================
    # Linaje (Lineage DAG)
    # ==========================================

    def get_lineage(self) -> List["RDD"]:
        """
        Retorna la lista ordenada de RDDs ancestros mediante recorrido en profundidad (DFS).
        Permite reconstruir particiones si un Worker muere (Resilience).
        """
        visited = set()
        lineage = []

        def _traverse(node: "RDD"):
            if node.id not in visited:
                visited.add(node.id)
                for parent in node.parents:
                    _traverse(parent)
                lineage.append(node)

        _traverse(self)
        return lineage

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "operation": self.operation,
            "num_partitions": self.num_partitions,
            "dependency_type": self.dependency_type,
            "is_cached": self.is_cached,
            "storage_level": self.storage_level,
            "parent_ids": [p.id for p in self.parents],
            "total_data_size_mb": round(self.total_data_size_mb, 2),
            "partitions": [p.to_dict() for p in self.partitions]
        }
