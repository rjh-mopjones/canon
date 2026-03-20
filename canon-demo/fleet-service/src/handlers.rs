use canon_core::command_handler;
use canon_demo_shared::commands::*;
use canon_demo_shared::events::*;

use crate::aggregate::{Ship, ShipStatus};
use crate::error::FleetError;

#[command_handler(Ship, version = 1)]
impl RegisterShipHandler {
    type Error = FleetError;

    fn handle(&self, _state: &Ship, cmd: RegisterShip) -> Result<ShipRegistered, FleetError> {
        Ok(ShipRegistered {
            ship_id: uuid::Uuid::new_v4(),
            name: cmd.name,
            capacity_kg: cmd.capacity_kg,
        })
    }
}

#[command_handler(Ship, version = 1)]
impl AssignRouteHandler {
    type Error = FleetError;

    fn handle(&self, _state: &Ship, cmd: AssignRoute) -> Result<RouteAssigned, FleetError> {
        Ok(RouteAssigned {
            ship_id: cmd.ship_id,
            route_id: cmd.route_id,
        })
    }
}

#[command_handler(Ship, version = 1)]
impl DepartForStationHandler {
    type Error = FleetError;

    fn handle(&self, state: &Ship, cmd: DepartForStation) -> Result<ShipDeparted, FleetError> {
        if state.status != ShipStatus::Docked {
            return Err(FleetError::ShipNotDocked);
        }
        Ok(ShipDeparted {
            ship_id: cmd.ship_id,
            destination: cmd.destination,
            fuel_at_departure: state.fuel_kg,
        })
    }
}

#[command_handler(Ship, version = 1)]
impl ScheduleResupplyHandler {
    type Error = FleetError;

    fn handle(
        &self,
        _state: &Ship,
        cmd: ScheduleResupply,
    ) -> Result<ResupplyScheduled, FleetError> {
        Ok(ResupplyScheduled {
            ship_id: cmd.ship_id,
            fuel_kg: cmd.fuel_kg,
        })
    }
}

#[command_handler(Ship, version = 1)]
impl DecommissionShipHandler {
    type Error = FleetError;

    fn handle(
        &self,
        state: &Ship,
        cmd: DecommissionShip,
    ) -> Result<ShipDecommissioned, FleetError> {
        if state.status == ShipStatus::Decommissioned {
            return Err(FleetError::AlreadyDecommissioned);
        }
        Ok(ShipDecommissioned {
            ship_id: cmd.ship_id,
        })
    }
}
