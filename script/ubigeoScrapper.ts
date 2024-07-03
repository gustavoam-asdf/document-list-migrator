import puppeteer from "puppeteer";

//https://prod3.seace.gob.pe/portalseace-uiwd-pub/ControllerServletOpen?pageid=0&portletid=BuscadorUbigeo&scriptdo=doView&page__ind=&v_dep_cod=01&v_pro_cod=01&v_dis_cod=01

const trueSource = 'https://prod3.seace.gob.pe/portalseace-uiwd-pub/ControllerServletOpen?pageid=0&portletid=BuscadorUbigeo&scriptdo=doView';

const browser = await puppeteer.launch();
const page = await browser.newPage();
await page.goto(trueSource);

type District = {
	code: string;
	value: string;
};

type Province = {
	code: string;
	value: string;
	disctricts: District[];
};

type Region = {
	code: string;
	value: string;
	provinces: Province[];
};

const exteriorRegion: Region = {
	code: "98",
	value: "EXTERIOR",
	provinces: [
		{
			code: "01",
			value: "EXTERIOR",
			disctricts: [
				{
					code: "00",
					value: "MULTIDISTRITAL"
				}
			]
		}
	]
}

const regionsWithoutProvinces = await page.evaluate(() => {
	const $regionsSelector = document.querySelector(`select[name="v_dep_cod"]`)!;
	const $regionOptions = $regionsSelector.querySelectorAll("option")!;

	const regionsWithoutProvinces = Array
		.from($regionOptions)
		.map($region => {
			const regionCode = $region.getAttribute("value")!;

			if (regionCode === "00" || regionCode === "98") {
				return null;
			}

			const regionValue = $region.textContent?.replace(`${regionCode}-`, "").trim()!;

			const region: Omit<Region, "provinces"> = {
				code: regionCode,
				value: regionValue
			}

			return region;
		})
		.filter(Boolean) as Omit<Region, "provinces">[];

	return regionsWithoutProvinces;
});

const regionsWithoutDistricts: (Omit<Region, "provinces"> & {
	provinces: Omit<Province, "disctricts">[];
})[] = []

for await (const regionWithoutProvinces of regionsWithoutProvinces) {
	const regionPage = await browser.newPage();
	const regionUrl = `${trueSource}&v_dep_cod=${regionWithoutProvinces.code}`;

	console.log(`Searching provinces for region ${regionWithoutProvinces.value}`);
	await regionPage.goto(regionUrl);

	const provincesWithOutDistricts: Omit<Province, "disctricts">[] = await regionPage.evaluate(() => {
		const $provincesSelector = document.querySelector(`select[name="v_pro_cod"]`);
		if (!$provincesSelector) {
			throw new Error("Provinces selector not found");
		}

		const $provinceOptions = $provincesSelector.querySelectorAll("option");
		if (!$provinceOptions) {
			throw new Error("Provinces options not found");
		}


		const provinces = Array
			.from($provinceOptions)
			.map($province => {
				const provinceCode = $province.getAttribute("value")!;

				if (provinceCode === "00") {
					return null;
				}

				const provinceValue = $province.textContent?.replace(`${provinceCode}-`, "").trim()!;

				const province: Omit<Province, "disctricts"> = {
					code: provinceCode,
					value: provinceValue
				}

				return province;
			})
			.filter(Boolean) as Omit<Province, "disctricts">[];

		return provinces;
	});

	regionsWithoutDistricts.push({
		...regionWithoutProvinces,
		provinces: provincesWithOutDistricts
	});
}

console.dir(regionsWithoutDistricts, {
	depth: null
})

await browser.close();